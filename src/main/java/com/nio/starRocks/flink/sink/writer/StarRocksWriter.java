// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.nio.starRocks.flink.sink.writer;

import com.nio.starRocks.flink.cfg.StarRocksExecutionOptions;
import com.nio.starRocks.flink.cfg.StarRocksOptions;
import com.nio.starRocks.flink.cfg.StarRocksReadOptions;
import com.nio.starRocks.flink.exception.StarRocksRuntimeException;
import com.nio.starRocks.flink.exception.StreamLoadException;
import com.nio.starRocks.flink.rest.RestService;
import com.nio.starRocks.flink.rest.models.RespContent;
import com.nio.starRocks.flink.sink.BackendUtil;
import com.nio.starRocks.flink.sink.StarRocksCommittable;
import com.nio.starRocks.flink.sink.HttpUtil;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.nio.starRocks.flink.sink.LoadStatus.PUBLISH_TIMEOUT;
import static com.nio.starRocks.flink.sink.LoadStatus.SUCCESS;

/**
 * StarRocks Writer will load data to starRocks.
 * @param <IN>
 */
public class StarRocksWriter<IN> implements SinkWriter<IN, StarRocksCommittable, StarRocksWriterState> {
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksWriter.class);
    private static final List<String> StarRocks_SUCCESS_STATUS = new ArrayList<>(Arrays.asList(SUCCESS, PUBLISH_TIMEOUT));
    private final long lastCheckpointId;
    private StarRocksStreamLoad starRocksStreamLoad;
    volatile boolean loading;
    private final StarRocksOptions starRocksOptions;
    private final StarRocksReadOptions starRocksReadOptions;
    private final StarRocksExecutionOptions executionOptions;
    private final String labelPrefix;
    private final LabelGenerator labelGenerator;
    private final int intervalTime;
    private final StarRocksWriterState starRocksWriterState;
    private final StarRocksRecordSerializer<IN> serializer;
    private final transient ScheduledExecutorService scheduledExecutorService;
    private transient Thread executorThread;
    private transient volatile Exception loadException = null;
    private BackendUtil backendUtil;
    private String currentLabel;

    public StarRocksWriter(Sink.InitContext initContext,
                       List<StarRocksWriterState> state,
                       StarRocksRecordSerializer<IN> serializer,
                       StarRocksOptions starRocksOptions,
                       StarRocksReadOptions starRocksReadOptions,
                       StarRocksExecutionOptions executionOptions) {
        this.lastCheckpointId =
                initContext
                        .getRestoredCheckpointId()
                        .orElse(CheckpointIDCounter.INITIAL_CHECKPOINT_ID - 1);
        LOG.info("restore checkpointId {}", lastCheckpointId);
        LOG.info("labelPrefix " + executionOptions.getLabelPrefix());
        this.starRocksWriterState = new StarRocksWriterState(executionOptions.getLabelPrefix());
        this.labelPrefix = executionOptions.getLabelPrefix() + "_" + initContext.getSubtaskId();
        this.labelGenerator = new LabelGenerator(labelPrefix, executionOptions.enabled2PC());
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(1, new ExecutorThreadFactory("stream-load-check"));
        this.serializer = serializer;
        this.starRocksOptions = starRocksOptions;
        this.starRocksReadOptions = starRocksReadOptions;
        this.executionOptions = executionOptions;
        this.intervalTime = executionOptions.checkInterval();
        this.loading = false;
    }

    public void initializeLoad(List<StarRocksWriterState> state) throws IOException {
        this.backendUtil = StringUtils.isNotEmpty(starRocksOptions.getBenodes()) ? new BackendUtil(
                starRocksOptions.getBenodes())
                : new BackendUtil(RestService.getBackendsV2(starRocksOptions, starRocksReadOptions, LOG));
        try {
            this.starRocksStreamLoad = new StarRocksStreamLoad(
                    backendUtil.getAvailableBackend(),
                    starRocksOptions,
                    executionOptions,
                    labelGenerator, new HttpUtil().getHttpClient());
            // TODO: we need check and abort all pending transaction.
            //  Discard transactions that may cause the job to fail.
            if(executionOptions.enabled2PC()) {
                starRocksStreamLoad.abortPreCommit(labelPrefix, lastCheckpointId + 1);
            }
        } catch (Exception e) {
            throw new StarRocksRuntimeException(e);
        }
        // get main work thread.
        executorThread = Thread.currentThread();
        this.currentLabel = labelGenerator.generateLabel(lastCheckpointId + 1);
        // when uploading data in streaming mode, we need to regularly detect whether there are exceptions.
        scheduledExecutorService.scheduleWithFixedDelay(this::checkDone, 200, intervalTime, TimeUnit.MILLISECONDS);
    }

    @Override
    public void write(IN in, Context context) throws IOException {
        checkLoadException();
        byte[] serialize = serializer.serialize(in);
        if(Objects.isNull(serialize)){
            //ddl record
            return;
        }
        if(!loading) {
            //Start streamload only when there has data
            starRocksStreamLoad.startLoad(currentLabel);
            loading = true;
        }
        starRocksStreamLoad.writeRecord(serialize);
    }

    @Override
    public List<StarRocksCommittable> prepareCommit(boolean flush) throws IOException {
        if(!loading){
            //There is no data during the entire checkpoint period
            return Collections.emptyList();
        }
        // disable exception checker before stop load.
        loading = false;
        Preconditions.checkState(starRocksStreamLoad != null);
        RespContent respContent = starRocksStreamLoad.stopLoad(currentLabel);
        if (!StarRocks_SUCCESS_STATUS.contains(respContent.getStatus())) {
            String errMsg = String.format("stream load error: %s, see more in %s", respContent.getMessage(), respContent.getErrorURL());
            throw new StarRocksRuntimeException(errMsg);
        }
        if (!executionOptions.enabled2PC()) {
            return Collections.emptyList();
        }
        long txnId = respContent.getTxnId();
        return ImmutableList.of(new StarRocksCommittable(starRocksStreamLoad.getHostPort(), starRocksStreamLoad.getDb(), txnId));
    }

    @Override
    public List<StarRocksWriterState> snapshotState(long checkpointId) throws IOException {
        Preconditions.checkState(starRocksStreamLoad != null);
        // dynamic refresh BE node
        this.starRocksStreamLoad.setHostPort(backendUtil.getAvailableBackend());
        this.currentLabel = labelGenerator.generateLabel(checkpointId + 1);
        return Collections.singletonList(starRocksWriterState);
    }

    private void checkDone() {
        // the load future is done and checked in prepareCommit().
        // this will check error while loading.
        LOG.debug("start timer checker, interval {} ms", intervalTime);
        if (starRocksStreamLoad.getPendingLoadFuture() != null
                && starRocksStreamLoad.getPendingLoadFuture().isDone()) {
            if (!loading) {
                LOG.debug("not loading, skip timer checker");
                return;
            }

            // double check to interrupt when loading is true and starRocksStreamLoad.getPendingLoadFuture().isDone
            // fix issue #139
            if (starRocksStreamLoad.getPendingLoadFuture() != null
                    && starRocksStreamLoad.getPendingLoadFuture().isDone()) {
                // TODO: introduce cache for reload instead of throwing exceptions.
                String errorMsg;
                try {
                    RespContent content = starRocksStreamLoad.handlePreCommitResponse(starRocksStreamLoad.getPendingLoadFuture().get());
                    errorMsg = content.getMessage();
                } catch (Exception e) {
                    errorMsg = e.getMessage();
                }

                loadException = new StreamLoadException(errorMsg);
                LOG.error("stream load finished unexpectedly, interrupt worker thread! {}", errorMsg);
                // set the executor thread interrupted in case blocking in write data.
                executorThread.interrupt();
            }
        }
    }

    private void checkLoadException() {
        if (loadException != null) {
            throw new RuntimeException("error while loading data.", loadException);
        }
    }

    @VisibleForTesting
    public boolean isLoading() {
        return this.loading;
    }

    @VisibleForTesting
    public void setStarRocksStreamLoad(StarRocksStreamLoad streamLoad) {
        this.starRocksStreamLoad = streamLoad;
    }

    @Override
    public void close() throws Exception {
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
        if (starRocksStreamLoad != null) {
            starRocksStreamLoad.close();
        }
    }
}

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

package com.nio.starRocks.flink.sink.batch;

import com.nio.starRocks.flink.cfg.StarRocksExecutionOptions;
import com.nio.starRocks.flink.cfg.StarRocksOptions;
import com.nio.starRocks.flink.cfg.StarRocksReadOptions;
import com.nio.starRocks.flink.sink.writer.StarRocksRecordSerializer;
import com.nio.starRocks.flink.sink.writer.LabelGenerator;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class StarRocksBatchWriter<IN> implements SinkWriter<IN> {
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksBatchWriter.class);
    private StarRocksBatchStreamLoad batchStreamLoad;
    private final StarRocksOptions starRocksOptions;
    private final StarRocksReadOptions starRocksReadOptions;
    private final StarRocksExecutionOptions executionOptions;
    private final String labelPrefix;
    private final LabelGenerator labelGenerator;
    private final long flushIntervalMs;
    private final StarRocksRecordSerializer<IN> serializer;
    private final transient ScheduledExecutorService scheduledExecutorService;
    private transient volatile Exception flushException = null;

    public StarRocksBatchWriter(Sink.InitContext initContext,
                                StarRocksRecordSerializer<IN> serializer,
                                StarRocksOptions starRocksOptions,
                                StarRocksReadOptions starRocksReadOptions,
                                StarRocksExecutionOptions executionOptions) {
        LOG.info("labelPrefix " + executionOptions.getLabelPrefix());
        this.labelPrefix = executionOptions.getLabelPrefix() + "_" + initContext.getSubtaskId();
        this.labelGenerator = new LabelGenerator(labelPrefix, false);
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(1, new ExecutorThreadFactory("stream-load-flush-interval"));
        this.serializer = serializer;
        this.starRocksOptions = starRocksOptions;
        this.starRocksReadOptions = starRocksReadOptions;
        this.executionOptions = executionOptions;
        this.flushIntervalMs = executionOptions.getBufferFlushIntervalMs();
    }

    public void initializeLoad() throws IOException {
        this.batchStreamLoad = new StarRocksBatchStreamLoad(starRocksOptions, starRocksReadOptions, executionOptions, labelGenerator);
        // when uploading data in streaming mode, we need to regularly detect whether there are exceptions.
        scheduledExecutorService.scheduleWithFixedDelay(this::intervalFlush, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
    }

    private void intervalFlush() {
        try {
            LOG.info("interval flush triggered.");
            batchStreamLoad.flush(false);
        } catch (InterruptedException e) {
            flushException = e;
        }
    }

    @Override
    public void write(IN in, Context context) throws IOException, InterruptedException {
        checkFlushException();
        byte[] serialize = serializer.serialize(in);
        if(Objects.isNull(serialize)){
            //ddl record
            return;
        }
        batchStreamLoad.writeRecord(serialize);
    }
    @Override
    public void flush(boolean flush) throws IOException, InterruptedException {
        checkFlushException();
        LOG.info("checkpoint flush triggered.");
        batchStreamLoad.flush(true);
    }

    @Override
    public void close() throws Exception {
        LOG.info("DorisBatchWriter Close");
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
        batchStreamLoad.close();
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("Writing records to streamload failed.", flushException);
        }
    }
}

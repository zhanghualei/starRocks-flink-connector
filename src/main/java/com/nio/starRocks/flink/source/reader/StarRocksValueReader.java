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

package com.nio.starRocks.flink.source.reader;

import com.nio.starRocks.flink.backend.BackendClient;
import com.nio.starRocks.flink.cfg.StarRocksOptions;
import com.nio.starRocks.flink.cfg.StarRocksReadOptions;
import com.nio.starRocks.flink.exception.StarRocksRuntimeException;
import com.nio.starRocks.flink.exception.IllegalArgumentException;
import com.nio.starRocks.flink.exception.ShouldNeverHappenException;
import com.nio.starRocks.flink.rest.PartitionDefinition;
import com.nio.starRocks.flink.rest.SchemaUtils;
import com.nio.starRocks.flink.rest.models.Schema;
import com.nio.starRocks.flink.serialization.Routing;
import com.nio.starRocks.flink.serialization.RowBatch;
import com.starrocks.thrift.TScanBatchResult;
import com.starrocks.thrift.TScanCloseParams;
import com.starrocks.thrift.TScanNextBatchParams;
import com.starrocks.thrift.TScanOpenParams;
import com.starrocks.thrift.TScanOpenResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.nio.starRocks.flink.cfg.ConfigurationOptions.STARROCKS_BATCH_SIZE_DEFAULT;
import static com.nio.starRocks.flink.cfg.ConfigurationOptions.STARROCKS_DEFAULT_CLUSTER;
import static com.nio.starRocks.flink.cfg.ConfigurationOptions.STARROCKS_DESERIALIZE_ARROW_ASYNC_DEFAULT;
import static com.nio.starRocks.flink.cfg.ConfigurationOptions.STARROCKS_DESERIALIZE_QUEUE_SIZE_DEFAULT;
import static com.nio.starRocks.flink.cfg.ConfigurationOptions.STARROCKS_EXEC_MEM_LIMIT_DEFAULT;
import static com.nio.starRocks.flink.cfg.ConfigurationOptions.STARROCKS_REQUEST_QUERY_TIMEOUT_S_DEFAULT;
import static com.nio.starRocks.flink.util.ErrorMessages.SHOULD_NOT_HAPPEN_MESSAGE;
import static com.starrocks.thrift.TScanOpenResult._Fields.CONTEXT_ID;

public class StarRocksValueReader implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksValueReader.class);
    protected BackendClient client;
    protected Lock clientLock = new ReentrantLock();

    private PartitionDefinition partition;
    private StarRocksOptions options;
    private StarRocksReadOptions readOptions;

    protected int offset = 0;
    protected AtomicBoolean eos = new AtomicBoolean(false);
    protected RowBatch rowBatch;

    // flag indicate if support deserialize Arrow to RowBatch asynchronously
    protected Boolean deserializeArrowToRowBatchAsync;

    protected BlockingQueue<RowBatch> rowBatchBlockingQueue;
    private TScanOpenParams openParams;
    protected String contextId;
    protected Schema schema;
    protected boolean asyncThreadStarted;

    public StarRocksValueReader(PartitionDefinition partition, StarRocksOptions options, StarRocksReadOptions readOptions) {
        this.partition = partition;
        this.options = options;
        this.readOptions = readOptions;
        this.client = backendClient();
        this.deserializeArrowToRowBatchAsync = readOptions.getDeserializeArrowAsync() == null ? STARROCKS_DESERIALIZE_ARROW_ASYNC_DEFAULT : readOptions.getDeserializeArrowAsync();

        Integer blockingQueueSize = readOptions.getDeserializeQueueSize() == null ? STARROCKS_DESERIALIZE_QUEUE_SIZE_DEFAULT : readOptions.getDeserializeQueueSize();
        if (this.deserializeArrowToRowBatchAsync) {
            this.rowBatchBlockingQueue = new ArrayBlockingQueue(blockingQueueSize);
        }
        init();
    }

//    private void init() {
//        clientLock.lock();
//        try {
//            this.openParams = openParams();
//            TScanOpenResult openResult = this.client.openScanner(this.openParams);
//            this.contextId = openResult.getContextId();
//            this.schema = SchemaUtils.convertToSchema(openResult.getSelectedColumns());
//        } finally {
//            clientLock.unlock();
//        }
//        this.asyncThreadStarted = asyncThreadStarted();
//        LOG.debug("Open scan result is, contextId: {}, schema: {}.", contextId, schema);
//    }

    private void init() {
        clientLock.lock();
        try {
            this.openParams = openParams();
            TScanOpenResult openResult = this.client.openScanner(this.openParams);
            this.contextId = openResult.getContext_id();
            this.schema = SchemaUtils.convertToSchema(openResult.getSelected_columns());
        } finally {
            clientLock.unlock();
        }
        this.asyncThreadStarted = asyncThreadStarted();
        LOG.debug("Open scan result is, contextId: {}, schema: {}.", contextId, schema);
    }


    private BackendClient backendClient() {
        try {
            return new BackendClient(new Routing(partition.getBeAddress()), readOptions);
        } catch (IllegalArgumentException e) {
            LOG.error("init backend:{} client failed,", partition.getBeAddress(), e);
            throw new StarRocksRuntimeException(e);
        }
    }

//    private TScanOpenParams openParams() {
//        TScanOpenParams params = new TScanOpenParams();
//        params.cluster = STARROCKS_DEFAULT_CLUSTER;
//        params.database = partition.getDatabase();
//        params.table = partition.getTable();
//
//        params.tablet_ids = Arrays.asList(partition.getTabletIds().toArray(new Long[]{}));
//        params.opaqued_query_plan = partition.getQueryPlan();
//        // max row number of one read batch
//        Integer batchSize = readOptions.getRequestBatchSize() == null ? STARROCKS_BATCH_SIZE_DEFAULT : readOptions.getRequestBatchSize();
//        Integer queryStarRocksTimeout = readOptions.getRequestQueryTimeoutS() == null ? STARROCKS_REQUEST_QUERY_TIMEOUT_S_DEFAULT : readOptions.getRequestQueryTimeoutS();
//        Long execMemLimit = readOptions.getExecMemLimit() == null ? STARROCKS_EXEC_MEM_LIMIT_DEFAULT : readOptions.getExecMemLimit();
//        params.setBatchSize(batchSize);
//        params.setQueryTimeout(queryStarRocksTimeout);
//        params.setMemLimit(execMemLimit);
//        params.setUser(options.getUsername());
//        params.setPasswd(options.getPassword());
//        LOG.debug("Open scan params is,cluster:{},database:{},table:{},tabletId:{},batch size:{},query timeout:{},execution memory limit:{},user:{},query plan: {}",
//                params.getCluster(), params.getDatabase(), params.getTable(), params.getTabletIds(), params.getBatchSize(), params.getQueryTimeout(), params.getMemLimit(), params.getUser(), params.getOpaquedQueryPlan());
//        return params;
//    }

    private TScanOpenParams openParams() {
        TScanOpenParams params = new TScanOpenParams();
        params.cluster = STARROCKS_DEFAULT_CLUSTER;
        params.database = partition.getDatabase();
        params.table = partition.getTable();

        params.tablet_ids = Arrays.asList(partition.getTabletIds().toArray(new Long[]{}));
        params.opaqued_query_plan = partition.getQueryPlan();
        // max row number of one read batch
        Integer batchSize = readOptions.getRequestBatchSize() == null ? STARROCKS_BATCH_SIZE_DEFAULT : readOptions.getRequestBatchSize();
        Integer queryStarRocksTimeout = readOptions.getRequestQueryTimeoutS() == null ? STARROCKS_REQUEST_QUERY_TIMEOUT_S_DEFAULT : readOptions.getRequestQueryTimeoutS();
        Long execMemLimit = readOptions.getExecMemLimit() == null ? STARROCKS_EXEC_MEM_LIMIT_DEFAULT : readOptions.getExecMemLimit();
        params.setBatch_size(batchSize);
        params.setQuery_timeout(queryStarRocksTimeout);
        params.setMem_limit(execMemLimit);
        params.setUser(options.getUsername());
        params.setPasswd(options.getPassword());
        LOG.debug("Open scan params is,cluster:{},database:{},table:{},tabletId:{},batch size:{},query timeout:{},execution memory limit:{},user:{},query plan: {}",
                params.getCluster(), params.getDatabase(), params.getTable(), params.getTablet_ids(), params.getBatch_size(), params.getQuery_timeout(), params.getMem_limit(), params.getUser(), params.getOpaqued_query_plan());
        return params;
    }

//    protected Thread asyncThread = new Thread(new Runnable() {
//        @Override
//        public void run() {
//            clientLock.lock();
//            try{
//                TScanNextBatchParams nextBatchParams = new TScanNextBatchParams();
//                nextBatchParams.setContextId(contextId);
//                while (!eos.get()) {
//                    nextBatchParams.setOffset(offset);
//                    TScanBatchResult nextResult = client.getNext(nextBatchParams);
//                    eos.set(nextResult.isEos());
//                    if (!eos.get()) {
//                        RowBatch rowBatch = new RowBatch(nextResult, schema).readArrow();
//                        offset += rowBatch.getReadRowCount();
//                        rowBatch.close();
//                        try {
//                            rowBatchBlockingQueue.put(rowBatch);
//                        } catch (InterruptedException e) {
//                            throw new StarRocksRuntimeException(e);
//                        }
//                    }
//                }
//            } finally {
//                clientLock.unlock();
//            }
//        }
//    });

    protected Thread asyncThread = new Thread(new Runnable() {
        @Override
        public void run() {
            clientLock.lock();
            try{
                TScanNextBatchParams nextBatchParams = new TScanNextBatchParams();
                nextBatchParams.setContext_id(contextId);
                while (!eos.get()) {
                    nextBatchParams.setOffset(offset);
                    TScanBatchResult nextResult = client.getNext(nextBatchParams);
                    eos.set(nextResult.isEos());
                    if (!eos.get()) {
                        RowBatch rowBatch = new RowBatch(nextResult, schema).readArrow();
                        offset += rowBatch.getReadRowCount();
                        rowBatch.close();
                        try {
                            rowBatchBlockingQueue.put(rowBatch);
                        } catch (InterruptedException e) {
                            throw new StarRocksRuntimeException(e);
                        }
                    }
                }
            } finally {
                clientLock.unlock();
            }
        }
    });

    protected boolean asyncThreadStarted() {
        boolean started = false;
        if (deserializeArrowToRowBatchAsync) {
            asyncThread.start();
            started = true;
        }
        return started;
    }

    /**
     * read data and cached in rowBatch.
     *
     * @return true if hax next value
     */
//    public boolean hasNext() {
//        boolean hasNext = false;
//        if (deserializeArrowToRowBatchAsync && asyncThreadStarted) {
//            // support deserialize Arrow to RowBatch asynchronously
//            if (rowBatch == null || !rowBatch.hasNext()) {
//                while (!eos.get() || !rowBatchBlockingQueue.isEmpty()) {
//                    if (!rowBatchBlockingQueue.isEmpty()) {
//                        try {
//                            rowBatch = rowBatchBlockingQueue.take();
//                        } catch (InterruptedException e) {
//                            throw new StarRocksRuntimeException(e);
//                        }
//                        hasNext = true;
//                        break;
//                    } else {
//                        // wait for rowBatch put in queue or eos change
//                        try {
//                            Thread.sleep(5);
//                        } catch (InterruptedException e) {
//                        }
//                    }
//                }
//            } else {
//                hasNext = true;
//            }
//        } else {
//            clientLock.lock();
//            try{
//                // Arrow data was acquired synchronously during the iterative process
//                if (!eos.get() && (rowBatch == null || !rowBatch.hasNext())) {
//                    if (rowBatch != null) {
//                        offset += rowBatch.getReadRowCount();
//                        rowBatch.close();
//                    }
//                    TScanNextBatchParams nextBatchParams = new TScanNextBatchParams();
//                    nextBatchParams.setContextId(contextId);
//                    nextBatchParams.setOffset(offset);
//                    TScanBatchResult nextResult = client.getNext(nextBatchParams);
//                    eos.set(nextResult.isEos());
//                    if (!eos.get()) {
//                        rowBatch = new RowBatch(nextResult, schema).readArrow();
//                    }
//                }
//                hasNext = !eos.get();
//            } finally {
//                clientLock.unlock();
//            }
//        }
//        return hasNext;
//    }

    public boolean hasNext() {
        boolean hasNext = false;
        if (deserializeArrowToRowBatchAsync && asyncThreadStarted) {
            // support deserialize Arrow to RowBatch asynchronously
            if (rowBatch == null || !rowBatch.hasNext()) {
                while (!eos.get() || !rowBatchBlockingQueue.isEmpty()) {
                    if (!rowBatchBlockingQueue.isEmpty()) {
                        try {
                            rowBatch = rowBatchBlockingQueue.take();
                        } catch (InterruptedException e) {
                            throw new StarRocksRuntimeException(e);
                        }
                        hasNext = true;
                        break;
                    } else {
                        // wait for rowBatch put in queue or eos change
                        try {
                            Thread.sleep(5);
                        } catch (InterruptedException e) {
                        }
                    }
                }
            } else {
                hasNext = true;
            }
        } else {
            clientLock.lock();
            try{
                // Arrow data was acquired synchronously during the iterative process
                if (!eos.get() && (rowBatch == null || !rowBatch.hasNext())) {
                    if (rowBatch != null) {
                        offset += rowBatch.getReadRowCount();
                        rowBatch.close();
                    }
                    TScanNextBatchParams nextBatchParams = new TScanNextBatchParams();
                    nextBatchParams.setContext_id(contextId);
                    nextBatchParams.setOffset(offset);
                    TScanBatchResult nextResult = client.getNext(nextBatchParams);
                    eos.set(nextResult.isEos());
                    if (!eos.get()) {
                        rowBatch = new RowBatch(nextResult, schema).readArrow();
                    }
                }
                hasNext = !eos.get();
            } finally {
                clientLock.unlock();
            }
        }
        return hasNext;
    }

    /**
     * get next value.
     *
     * @return next value
     */
    public List next() {
        if (!hasNext()) {
            LOG.error(SHOULD_NOT_HAPPEN_MESSAGE);
            throw new ShouldNeverHappenException();
        }
        return rowBatch.next();
    }

//    @Override
//    public void close() throws Exception {
//        clientLock.lock();
//        try {
//            TScanCloseParams closeParams = new TScanCloseParams();
//            closeParams.setContextId(contextId);
//            client.closeScanner(closeParams);
//        } finally {
//            clientLock.unlock();
//        }
//    }

    @Override
    public void close() throws Exception {
        clientLock.lock();
        try {
            TScanCloseParams closeParams = new TScanCloseParams();
            closeParams.setContext_id(contextId);
            client.closeScanner(closeParams);
        } finally {
            clientLock.unlock();
        }
    }
}

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
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

public class StarRocksBatchSink<IN> implements Sink<IN> {
    private final StarRocksOptions starRocksOptions;
    private final StarRocksReadOptions starRocksReadOptions;
    private final StarRocksExecutionOptions starRocksExecutionOptions;
    private final StarRocksRecordSerializer<IN> serializer;

    public StarRocksBatchSink(StarRocksOptions starRocksOptions,
                          StarRocksReadOptions starRocksReadOptions,
                          StarRocksExecutionOptions starRocksExecutionOptions,
                          StarRocksRecordSerializer<IN> serializer) {
        this.starRocksOptions = starRocksOptions;
        this.starRocksReadOptions = starRocksReadOptions;
        this.starRocksExecutionOptions = starRocksExecutionOptions;
        this.serializer = serializer;
    }

    @Override
    public SinkWriter<IN> createWriter(InitContext initContext) throws IOException {
        StarRocksBatchWriter<IN> starRocksBatchWriter = new StarRocksBatchWriter<IN>(initContext, serializer, starRocksOptions, starRocksReadOptions, starRocksExecutionOptions);
        starRocksBatchWriter.initializeLoad();
        return starRocksBatchWriter;
    }

    public static <IN> Builder<IN> builder() {
        return new Builder<>();
    }

    /**
     * build for StarRocksBatchSink.
     * @param <IN> record type.
     */
    public static class Builder<IN> {
        private StarRocksOptions starRocksOptions;
        private StarRocksReadOptions starRocksReadOptions;
        private StarRocksExecutionOptions starRocksExecutionOptions;
        private StarRocksRecordSerializer<IN> serializer;

        public Builder<IN> setStarRocksOptions(StarRocksOptions starRocksOptions) {
            this.starRocksOptions = starRocksOptions;
            return this;
        }

        public Builder<IN> setStarRocksReadOptions(StarRocksReadOptions starRocksReadOptions) {
            this.starRocksReadOptions = starRocksReadOptions;
            return this;
        }

        public Builder<IN> setStarRocksExecutionOptions(StarRocksExecutionOptions starRocksExecutionOptions) {
            this.starRocksExecutionOptions = starRocksExecutionOptions;
            return this;
        }

        public Builder<IN> setSerializer(StarRocksRecordSerializer<IN> serializer) {
            this.serializer = serializer;
            return this;
        }

        public StarRocksBatchSink<IN> build() {
            Preconditions.checkNotNull(starRocksOptions);
            Preconditions.checkNotNull(starRocksExecutionOptions);
            Preconditions.checkNotNull(serializer);
            if(starRocksReadOptions == null) {
                starRocksReadOptions = StarRocksReadOptions.builder().build();
            }
            return new StarRocksBatchSink<>(starRocksOptions, starRocksReadOptions, starRocksExecutionOptions, serializer);
        }
    }
}

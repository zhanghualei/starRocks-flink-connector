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

package com.nio.starRocks.flink.sink;

import com.nio.starRocks.flink.cfg.StarRocksExecutionOptions;
import com.nio.starRocks.flink.cfg.StarRocksOptions;
import com.nio.starRocks.flink.cfg.StarRocksReadOptions;
import com.nio.starRocks.flink.sink.committer.StarRocksCommitter;
import com.nio.starRocks.flink.sink.writer.StarRocksRecordSerializer;
import com.nio.starRocks.flink.sink.writer.StarRocksWriter;
import org.apache.flink.api.connector.sink.Committer;
import com.nio.starRocks.flink.sink.writer.StarRocksWriterState;
import com.nio.starRocks.flink.sink.writer.StarRocksWriterStateSerializer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Load data into starRocks based on 2PC.
 * see {@link StarRocksWriter} and {@link StarRocksCommitter}.
 * @param <IN> type of record.
 */
public class StarRocksSink<IN> implements Sink<IN, StarRocksCommittable, StarRocksWriterState, StarRocksCommittable> {

    private final StarRocksOptions starRocksOptions;
    private final StarRocksReadOptions starRocksReadOptions;
    private final StarRocksExecutionOptions starRocksExecutionOptions;
    private final StarRocksRecordSerializer<IN> serializer;

    public StarRocksSink(StarRocksOptions starRocksOptions,
                         StarRocksReadOptions starRocksReadOptions,
                         StarRocksExecutionOptions starRocksExecutionOptions,
                         StarRocksRecordSerializer<IN> serializer) {
        this.starRocksOptions = starRocksOptions;
        this.starRocksReadOptions = starRocksReadOptions;
        this.starRocksExecutionOptions = starRocksExecutionOptions;
        this.serializer = serializer;
    }

    @Override
    public SinkWriter<IN, StarRocksCommittable, StarRocksWriterState> createWriter(InitContext initContext, List<StarRocksWriterState> state) throws IOException {
        StarRocksWriter<IN> starRocksWriter = new StarRocksWriter<IN>(initContext, state, serializer, starRocksOptions, starRocksReadOptions, starRocksExecutionOptions);
        starRocksWriter.initializeLoad(state);
        return starRocksWriter;
    }

    @Override
    public Optional<SimpleVersionedSerializer<StarRocksWriterState>> getWriterStateSerializer() {
        return Optional.of(new StarRocksWriterStateSerializer());
    }

    @Override
    public Optional<Committer<StarRocksCommittable>> createCommitter() throws IOException {
        return Optional.of(new StarRocksCommitter(starRocksOptions, starRocksReadOptions, starRocksExecutionOptions.getMaxRetries()));
    }

    @Override
    public Optional<GlobalCommitter<StarRocksCommittable, StarRocksCommittable>> createGlobalCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<StarRocksCommittable>> getCommittableSerializer() {
        return Optional.of(new StarRocksCommittableSerializer());
    }

    @Override
    public Optional<SimpleVersionedSerializer<StarRocksCommittable>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    public static <IN> Builder<IN> builder() {
        return new Builder<>();
    }

    /**
     * build for starRocksSink.
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

        public StarRocksSink<IN> build() {
            Preconditions.checkNotNull(starRocksOptions);
            Preconditions.checkNotNull(starRocksExecutionOptions);
            Preconditions.checkNotNull(serializer);
            if(starRocksReadOptions == null) {
                starRocksReadOptions = StarRocksReadOptions.builder().build();
            }
            return new StarRocksSink<>(starRocksOptions, starRocksReadOptions, starRocksExecutionOptions, serializer);
        }
    }
}

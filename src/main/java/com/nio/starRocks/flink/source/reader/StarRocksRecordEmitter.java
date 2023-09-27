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

import com.nio.starRocks.flink.deserialization.StarRocksDeserializationSchema;
import com.nio.starRocks.flink.source.split.StarRocksSourceSplitState;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * The {@link RecordEmitter} implementation for {@link StarRocksSourceReader}.
 **/
public class StarRocksRecordEmitter<T>
        implements RecordEmitter<List, T, StarRocksSourceSplitState> {

    private final StarRocksDeserializationSchema<T> starRocksDeserializationSchema;
    private final OutputCollector<T> outputCollector;


    public StarRocksRecordEmitter(StarRocksDeserializationSchema<T> starRocksDeserializationSchema) {
        this.starRocksDeserializationSchema = starRocksDeserializationSchema;
        this.outputCollector = new OutputCollector<>();
    }


    @Override
    public void emitRecord(List value, SourceOutput<T> output, StarRocksSourceSplitState splitState) throws Exception {
        outputCollector.output = output;
        starRocksDeserializationSchema.deserialize(value, outputCollector);
    }

    private static class OutputCollector<T> implements Collector<T> {
        private SourceOutput<T> output;

        @Override
        public void collect(T record) {
            output.collect(record);
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}

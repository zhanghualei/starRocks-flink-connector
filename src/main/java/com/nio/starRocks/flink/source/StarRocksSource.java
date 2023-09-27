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
package com.nio.starRocks.flink.source;

import com.nio.starRocks.flink.cfg.StarRocksOptions;
import com.nio.starRocks.flink.cfg.StarRocksReadOptions;
import com.nio.starRocks.flink.deserialization.StarRocksDeserializationSchema;
import com.nio.starRocks.flink.rest.PartitionDefinition;
import com.nio.starRocks.flink.rest.RestService;
import com.nio.starRocks.flink.source.assigners.StarRocksSplitAssigner;
import com.nio.starRocks.flink.source.assigners.SimpleSplitAssigner;
import com.nio.starRocks.flink.source.enumerator.StarRocksSourceEnumerator;
import com.nio.starRocks.flink.source.enumerator.PendingSplitsCheckpoint;
import com.nio.starRocks.flink.source.enumerator.PendingSplitsCheckpointSerializer;
import com.nio.starRocks.flink.source.reader.StarRocksRecordEmitter;
import com.nio.starRocks.flink.source.reader.StarRocksSourceReader;
import com.nio.starRocks.flink.source.split.StarRocksSourceSplit;
import com.nio.starRocks.flink.source.split.StarRocksSourceSplitSerializer;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * StarRocksSource based on FLIP-27 which is a BOUNDED stream.
 **/
public class StarRocksSource<OUT> implements Source<OUT, StarRocksSourceSplit, PendingSplitsCheckpoint>,
        ResultTypeQueryable<OUT> {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSource.class);

    private final StarRocksOptions options;
    private final StarRocksReadOptions readOptions;

    // Boundedness
    private final Boundedness boundedness;
    private final StarRocksDeserializationSchema<OUT> deserializer;

    public StarRocksSource(StarRocksOptions options,
                       StarRocksReadOptions readOptions,
                       Boundedness boundedness,
                       StarRocksDeserializationSchema<OUT> deserializer) {
        this.options = options;
        this.readOptions = readOptions;
        this.boundedness = boundedness;
        this.deserializer = deserializer;
    }

    @Override
    public Boundedness getBoundedness() {
        return this.boundedness;
    }

    @Override
    public SourceReader<OUT, StarRocksSourceSplit> createReader(SourceReaderContext readerContext) throws Exception {
        return new StarRocksSourceReader<>(
                options,
                readOptions,
                new StarRocksRecordEmitter<>(deserializer),
                readerContext,
                readerContext.getConfiguration()
        );
    }

    @Override
    public SplitEnumerator<StarRocksSourceSplit, PendingSplitsCheckpoint> createEnumerator(SplitEnumeratorContext<StarRocksSourceSplit> context) throws Exception {
        List<StarRocksSourceSplit> starRocksSourceSplits = new ArrayList<>();
        List<PartitionDefinition> partitions = RestService.findPartitions(options, readOptions, LOG);
        partitions.forEach(m -> starRocksSourceSplits.add(new StarRocksSourceSplit(m)));
        StarRocksSplitAssigner splitAssigner = new SimpleSplitAssigner(starRocksSourceSplits);

        return new StarRocksSourceEnumerator(context, splitAssigner);
    }

    @Override
    public SplitEnumerator<StarRocksSourceSplit, PendingSplitsCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<StarRocksSourceSplit> context,
            PendingSplitsCheckpoint checkpoint) throws Exception {
        Collection<StarRocksSourceSplit> splits = checkpoint.getSplits();
        StarRocksSplitAssigner splitAssigner = new SimpleSplitAssigner(splits);
        return new StarRocksSourceEnumerator(context, splitAssigner);
    }

    @Override
    public SimpleVersionedSerializer<StarRocksSourceSplit> getSplitSerializer() {
        return StarRocksSourceSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<PendingSplitsCheckpoint> getEnumeratorCheckpointSerializer() {
        return new PendingSplitsCheckpointSerializer(getSplitSerializer());
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return deserializer.getProducedType();
    }

    public static <OUT> StarRocksSourceBuilder<OUT> builder() {
        return new StarRocksSourceBuilder();
    }

    /**
     * build for StarRocksSource.
     * @param <OUT> record type.
     */

    public static class StarRocksSourceBuilder<OUT> {

        private StarRocksOptions options;
        private StarRocksReadOptions readOptions;

        // Boundedness
        private Boundedness boundedness;
        private StarRocksDeserializationSchema<OUT> deserializer;

        StarRocksSourceBuilder() {
            boundedness = Boundedness.BOUNDED;
        }


        public StarRocksSourceBuilder<OUT> setStarRocksOptions(StarRocksOptions options) {
            this.options = options;
            return this;
        }

        public StarRocksSourceBuilder<OUT> setStarRocksReadOptions(StarRocksReadOptions readOptions) {
            this.readOptions = readOptions;
            return this;
        }

        public StarRocksSourceBuilder<OUT> setBoundedness(Boundedness boundedness) {
            this.boundedness = boundedness;
            return this;
        }

        public StarRocksSourceBuilder<OUT> setDeserializer(StarRocksDeserializationSchema<OUT> deserializer) {
            this.deserializer = deserializer;
            return this;
        }

        public StarRocksSource<OUT> build() {
            if(readOptions == null){
                readOptions = StarRocksReadOptions.builder().build();
            }
            return new StarRocksSource<>(options, readOptions, boundedness, deserializer);
        }
    }

}

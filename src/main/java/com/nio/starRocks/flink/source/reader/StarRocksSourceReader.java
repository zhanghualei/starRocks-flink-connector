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

import com.nio.starRocks.flink.cfg.StarRocksOptions;
import com.nio.starRocks.flink.cfg.StarRocksReadOptions;
import com.nio.starRocks.flink.source.split.StarRocksSourceSplit;
import com.nio.starRocks.flink.source.split.StarRocksSourceSplitState;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;

import java.util.List;
import java.util.Map;

/**
 * A {@link SourceReader} that read records from {@link StarRocksSourceSplit}.
 **/
public class StarRocksSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<List, T, StarRocksSourceSplit, StarRocksSourceSplitState> {


    public StarRocksSourceReader(StarRocksOptions options,
                             StarRocksReadOptions readOptions,
                             RecordEmitter<List, T, StarRocksSourceSplitState> recordEmitter,
                             SourceReaderContext context,
                             Configuration config) {
        super(() -> new StarRocksSourceSplitReader(options, readOptions), recordEmitter, config, context);
    }

    @Override
    public void start() {
        // we request a split only if we did not get splits during the checkpoint restore
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected void onSplitFinished(Map<String, StarRocksSourceSplitState> finishedSplitIds) {
        context.sendSplitRequest();
    }

    @Override
    protected StarRocksSourceSplitState initializedState(StarRocksSourceSplit split) {
        return new StarRocksSourceSplitState(split);
    }

    @Override
    protected StarRocksSourceSplit toSplitType(String splitId, StarRocksSourceSplitState splitState) {
        return splitState.toStarRocksSourceSplit();
    }
}

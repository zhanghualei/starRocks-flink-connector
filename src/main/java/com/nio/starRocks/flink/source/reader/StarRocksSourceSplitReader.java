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
import com.nio.starRocks.flink.source.split.StarRocksSplitRecords;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

/**
 * The {@link SplitReader} implementation for the starRocks source.
 **/
public class StarRocksSourceSplitReader
        implements SplitReader<List, StarRocksSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSourceSplitReader.class);

    private final Queue<StarRocksSourceSplit> splits;
    private final StarRocksOptions options;
    private final StarRocksReadOptions readOptions;
    private StarRocksValueReader valueReader;
    private String currentSplitId;

    public StarRocksSourceSplitReader(StarRocksOptions options, StarRocksReadOptions readOptions) {
        this.options = options;
        this.readOptions = readOptions;
        this.splits = new ArrayDeque<>();
    }

    @Override
    public RecordsWithSplitIds<List> fetch() throws IOException {
        checkSplitOrStartNext();

        if (!valueReader.hasNext()) {
            return finishSplit();
        }
        return StarRocksSplitRecords.forRecords(currentSplitId, valueReader);
    }

    private void checkSplitOrStartNext() throws IOException {
        if (valueReader != null) {
            return;
        }
        final StarRocksSourceSplit nextSplit = splits.poll();
        if (nextSplit == null) {
            throw new IOException("Cannot fetch from another split - no split remaining");
        }
        currentSplitId = nextSplit.splitId();
        valueReader = new StarRocksValueReader(nextSplit.getPartitionDefinition(), options, readOptions);
    }

    private StarRocksSplitRecords finishSplit() {
        final StarRocksSplitRecords finishRecords = StarRocksSplitRecords.finishedSplit(currentSplitId);
        currentSplitId = null;
        return finishRecords;
    }

    @Override
    public void handleSplitsChanges(SplitsChange<StarRocksSourceSplit> splitsChange) {
        LOG.debug("Handling split change {}", splitsChange);
        splits.addAll(splitsChange.splits());
    }

    @Override
    public void wakeUp() {
    }

    @Override
    public void close() throws Exception {
        if (valueReader != null) {
            valueReader.close();
        }
    }
}

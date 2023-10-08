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

package com.nio.starRocks.flink.table;

import com.nio.starRocks.flink.cfg.StarRocksExecutionOptions;
import com.nio.starRocks.flink.cfg.StarRocksOptions;
import com.nio.starRocks.flink.cfg.StarRocksReadOptions;
import com.nio.starRocks.flink.cfg.StarRocksExecutionOptions;
import com.nio.starRocks.flink.cfg.StarRocksOptions;
import com.nio.starRocks.flink.cfg.StarRocksReadOptions;
import com.nio.starRocks.flink.rest.RestService;
import com.nio.starRocks.flink.sink.StarRocksSink;
import com.nio.starRocks.flink.sink.batch.StarRocksBatchSink;
import com.nio.starRocks.flink.sink.writer.RowDataSerializer;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkProvider;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.nio.starRocks.flink.sink.writer.LoadConstants.COLUMNS_KEY;
import static com.nio.starRocks.flink.sink.writer.LoadConstants.CSV;
import static com.nio.starRocks.flink.sink.writer.LoadConstants.STARROCKS_DELETE_SIGN;
import static com.nio.starRocks.flink.sink.writer.LoadConstants.FIELD_DELIMITER_DEFAULT;
import static com.nio.starRocks.flink.sink.writer.LoadConstants.FIELD_DELIMITER_KEY;
import static com.nio.starRocks.flink.sink.writer.LoadConstants.FORMAT_KEY;

/**
 * StarRocksDynamicTableSink
 **/
public class StarRocksDynamicTableSink implements DynamicTableSink {
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksDynamicTableSink.class);
    private final StarRocksOptions options;
    private final StarRocksReadOptions readOptions;
    private final StarRocksExecutionOptions executionOptions;
    private final TableSchema tableSchema;
    private final Integer sinkParallelism;

    public StarRocksDynamicTableSink(StarRocksOptions options,
                                     StarRocksReadOptions readOptions,
                                     StarRocksExecutionOptions executionOptions,
                                 TableSchema tableSchema,
                                 Integer sinkParallelism) {
        this.options = options;
        this.readOptions = readOptions;
        this.executionOptions = executionOptions;
        this.tableSchema = tableSchema;
        this.sinkParallelism = sinkParallelism;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        if(executionOptions.getIgnoreUpdateBefore()){
            return ChangelogMode.upsert();
        }else{
            return ChangelogMode.all();
        }
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        Properties loadProperties = executionOptions.getStreamLoadProp();
        boolean deletable = executionOptions.getDeletable() && RestService.isUniqueKeyType(options, readOptions, LOG);
        if (!loadProperties.containsKey(COLUMNS_KEY)) {
            String[] fieldNames = tableSchema.getFieldNames();
            Preconditions.checkState(fieldNames != null && fieldNames.length > 0);
            String columns = String.join(",", Arrays.stream(fieldNames).map(item -> String.format("`%s`", item.trim().replace("`", ""))).collect(Collectors.toList()));
            if (deletable) {
                columns = String.format("%s,%s", columns, STARROCKS_DELETE_SIGN);
            }
            loadProperties.put(COLUMNS_KEY, columns);
        }

        RowDataSerializer.Builder serializerBuilder = RowDataSerializer.builder();
        serializerBuilder.setFieldNames(tableSchema.getFieldNames())
                .setFieldType(tableSchema.getFieldDataTypes())
                .setType(loadProperties.getProperty(FORMAT_KEY, CSV))
                .enableDelete(deletable)
                .setFieldDelimiter(loadProperties.getProperty(FIELD_DELIMITER_KEY, FIELD_DELIMITER_DEFAULT));

        if(!executionOptions.enableBatchMode()){
            StarRocksSink.Builder<RowData> starRocksSinkBuilder = StarRocksSink.builder();
            starRocksSinkBuilder.setStarRocksOptions(options)
                    .setStarRocksReadOptions(readOptions)
                    .setStarRocksExecutionOptions(executionOptions)
                    .setSerializer(serializerBuilder.build());
            return SinkProvider.of(starRocksSinkBuilder.build(), sinkParallelism);
        }else{
            StarRocksBatchSink.Builder<RowData> starRocksBatchSinkBuilder = StarRocksBatchSink.builder();
            starRocksBatchSinkBuilder.setStarRocksOptions(options)
                    .setStarRocksReadOptions(readOptions)
                    .setStarRocksExecutionOptions(executionOptions)
                    .setSerializer(serializerBuilder.build());
            return SinkV2Provider.of(starRocksBatchSinkBuilder.build(), sinkParallelism);
        }
    }

    @Override
    public DynamicTableSink copy() {
        return new StarRocksDynamicTableSink(options, readOptions, executionOptions, tableSchema, sinkParallelism);
    }

    @Override
    public String asSummaryString() {
        return "StarRocks Table Sink";
    }
}

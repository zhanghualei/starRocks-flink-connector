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
import com.nio.starRocks.flink.cfg.StarRocksLookupOptions;
import com.nio.starRocks.flink.cfg.StarRocksOptions;
import com.nio.starRocks.flink.cfg.StarRocksReadOptions;

import static com.nio.starRocks.flink.table.StarRocksConfigOptions.BENODES;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.nio.starRocks.flink.table.StarRocksConfigOptions.STARROCKS_BATCH_SIZE;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.STARROCKS_DESERIALIZE_ARROW_ASYNC;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.STARROCKS_DESERIALIZE_QUEUE_SIZE;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.STARROCKS_EXEC_MEM_LIMIT;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.STARROCKS_FILTER_QUERY;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.STARROCKS_READ_FIELD;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.STARROCKS_REQUEST_CONNECT_TIMEOUT_MS;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.STARROCKS_REQUEST_QUERY_TIMEOUT_S;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.STARROCKS_REQUEST_READ_TIMEOUT_MS;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.STARROCKS_REQUEST_RETRIES;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.STARROCKS_TABLET_SIZE;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.FENODES;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.IDENTIFIER;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.JDBC_URL;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.LOOKUP_CACHE_MAX_ROWS;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.LOOKUP_CACHE_TTL;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.LOOKUP_JDBC_ASYNC;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.LOOKUP_JDBC_READ_BATCH_QUEUE_SIZE;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.LOOKUP_JDBC_READ_BATCH_SIZE;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.LOOKUP_JDBC_READ_THREAD_SIZE;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.LOOKUP_MAX_RETRIES;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.PASSWORD;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.SINK_BUFFER_COUNT;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.SINK_BUFFER_FLUSH_INTERVAL;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.SINK_BUFFER_FLUSH_MAX_BYTES;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.SINK_BUFFER_SIZE;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.SINK_CHECK_INTERVAL;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.SINK_ENABLE_2PC;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.SINK_ENABLE_BATCH_MODE;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.SINK_ENABLE_DELETE;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.SINK_FLUSH_QUEUE_SIZE;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.SINK_IGNORE_UPDATE_BEFORE;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.SINK_LABEL_PREFIX;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.SINK_MAX_RETRIES;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.SINK_PARALLELISM;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.SOURCE_USE_OLD_API;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.STREAM_LOAD_PROP_PREFIX;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.TABLE_IDENTIFIER;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.USERNAME;


/**
 * The {@link StarRocksDynamicTableFactory} translates the catalog table to a table source.
 *
 * <p>Because the table source requires a decoding format, we are discovering the format using the
 * provided {@link FactoryUtil} for convenience.
 */
public final class StarRocksDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER; // used for matching to `connector = '...'`
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FENODES);
        options.add(TABLE_IDENTIFIER);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FENODES);
        options.add(BENODES);
        options.add(TABLE_IDENTIFIER);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(JDBC_URL);

        options.add(STARROCKS_READ_FIELD);
        options.add(STARROCKS_FILTER_QUERY);
        options.add(STARROCKS_TABLET_SIZE);
        options.add(STARROCKS_REQUEST_CONNECT_TIMEOUT_MS);
        options.add(STARROCKS_REQUEST_READ_TIMEOUT_MS);
        options.add(STARROCKS_REQUEST_QUERY_TIMEOUT_S);
        options.add(STARROCKS_REQUEST_RETRIES);
        options.add(STARROCKS_DESERIALIZE_ARROW_ASYNC);
        options.add(STARROCKS_DESERIALIZE_QUEUE_SIZE);
        options.add(STARROCKS_BATCH_SIZE);
        options.add(STARROCKS_EXEC_MEM_LIMIT);
        options.add(LOOKUP_CACHE_MAX_ROWS);
        options.add(LOOKUP_CACHE_TTL);
        options.add(LOOKUP_MAX_RETRIES);
        options.add(LOOKUP_JDBC_ASYNC);
        options.add(LOOKUP_JDBC_READ_BATCH_SIZE);
        options.add(LOOKUP_JDBC_READ_THREAD_SIZE);
        options.add(LOOKUP_JDBC_READ_BATCH_QUEUE_SIZE);

        options.add(SINK_CHECK_INTERVAL);
        options.add(SINK_ENABLE_2PC);
        options.add(SINK_MAX_RETRIES);
        options.add(SINK_ENABLE_DELETE);
        options.add(SINK_LABEL_PREFIX);
        options.add(SINK_BUFFER_SIZE);
        options.add(SINK_BUFFER_COUNT);
        options.add(SINK_PARALLELISM);
        options.add(SINK_IGNORE_UPDATE_BEFORE);

        options.add(SINK_ENABLE_BATCH_MODE);
        options.add(SINK_BUFFER_FLUSH_MAX_ROWS);
        options.add(SINK_BUFFER_FLUSH_MAX_BYTES);
        options.add(SINK_FLUSH_QUEUE_SIZE);
        options.add(SINK_BUFFER_FLUSH_INTERVAL);

        options.add(SOURCE_USE_OLD_API);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        // either implement your custom validation logic here ...
        // or use the provided helper utility
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        // validate all options
        helper.validateExcept(STREAM_LOAD_PROP_PREFIX);
        // get the validated options
        final ReadableConfig options = helper.getOptions();
        // derive the produced data type (excluding computed columns) from the catalog table
        final DataType producedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();
        TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        // create and return dynamic table source
        return new StarRocksDynamicTableSource(
                getDorisOptions(helper.getOptions()),
                getDorisReadOptions(helper.getOptions()),
                getDorisLookupOptions(helper.getOptions()),
                physicalSchema);
    }

    private StarRocksOptions getDorisOptions(ReadableConfig readableConfig) {
        final String fenodes = readableConfig.get(FENODES);
        final String benodes = readableConfig.get(BENODES);
        final StarRocksOptions.Builder builder = StarRocksOptions.builder()
                .setFenodes(fenodes)
                .setBenodes(benodes)
                .setJdbcUrl(readableConfig.get(JDBC_URL))
                .setTableIdentifier(readableConfig.get(TABLE_IDENTIFIER));

        readableConfig.getOptional(USERNAME).ifPresent(builder::setUsername);
        readableConfig.getOptional(PASSWORD).ifPresent(builder::setPassword);
        return builder.build();
    }

    private StarRocksReadOptions getDorisReadOptions(ReadableConfig readableConfig) {
        final StarRocksReadOptions.Builder builder = StarRocksReadOptions.builder();
        builder.setDeserializeArrowAsync(readableConfig.get(STARROCKS_DESERIALIZE_ARROW_ASYNC))
                .setDeserializeQueueSize(readableConfig.get(STARROCKS_DESERIALIZE_QUEUE_SIZE))
                .setExecMemLimit(readableConfig.get(STARROCKS_EXEC_MEM_LIMIT))
                .setFilterQuery(readableConfig.get(STARROCKS_FILTER_QUERY))
                .setReadFields(readableConfig.get(STARROCKS_READ_FIELD))
                .setRequestQueryTimeoutS(readableConfig.get(STARROCKS_REQUEST_QUERY_TIMEOUT_S))
                .setRequestBatchSize(readableConfig.get(STARROCKS_BATCH_SIZE))
                .setRequestConnectTimeoutMs(readableConfig.get(STARROCKS_REQUEST_CONNECT_TIMEOUT_MS))
                .setRequestReadTimeoutMs(readableConfig.get(STARROCKS_REQUEST_READ_TIMEOUT_MS))
                .setRequestRetries(readableConfig.get(STARROCKS_REQUEST_RETRIES))
                .setRequestTabletSize(readableConfig.get(STARROCKS_TABLET_SIZE))
                .setUseOldApi(readableConfig.get(SOURCE_USE_OLD_API));
        return builder.build();
    }

    private StarRocksExecutionOptions getDorisExecutionOptions(ReadableConfig readableConfig, Properties streamLoadProp) {
        final StarRocksExecutionOptions.Builder builder = StarRocksExecutionOptions.builder();
        builder.setCheckInterval(readableConfig.get(SINK_CHECK_INTERVAL));
        builder.setMaxRetries(readableConfig.get(SINK_MAX_RETRIES));
        builder.setBufferSize(readableConfig.get(SINK_BUFFER_SIZE));
        builder.setBufferCount(readableConfig.get(SINK_BUFFER_COUNT));
        builder.setLabelPrefix(readableConfig.get(SINK_LABEL_PREFIX));
        builder.setStreamLoadProp(streamLoadProp);
        builder.setDeletable(readableConfig.get(SINK_ENABLE_DELETE));
        builder.setIgnoreUpdateBefore(readableConfig.get(SINK_IGNORE_UPDATE_BEFORE));
        if (!readableConfig.get(SINK_ENABLE_2PC)) {
            builder.disable2PC();
        }

        if(readableConfig.get(SINK_ENABLE_BATCH_MODE)) {
            builder.enableBatchMode();
        }

        builder.setFlushQueueSize(readableConfig.get(SINK_FLUSH_QUEUE_SIZE));
        builder.setBufferFlushMaxRows(readableConfig.get(SINK_BUFFER_FLUSH_MAX_ROWS));
        builder.setBufferFlushMaxBytes(readableConfig.get(SINK_BUFFER_FLUSH_MAX_BYTES));
        builder.setBufferFlushIntervalMs(readableConfig.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis());
        return builder.build();
    }

    private Properties getStreamLoadProp(Map<String, String> tableOptions) {
        final Properties streamLoadProp = new Properties();

        for (Map.Entry<String, String> entry : tableOptions.entrySet()) {
            if (entry.getKey().startsWith(STREAM_LOAD_PROP_PREFIX)) {
                String subKey = entry.getKey().substring(STREAM_LOAD_PROP_PREFIX.length());
                streamLoadProp.put(subKey, entry.getValue());
            }
        }
        return streamLoadProp;
    }

    private StarRocksLookupOptions getDorisLookupOptions(ReadableConfig readableConfig){
        final StarRocksLookupOptions.Builder builder = StarRocksLookupOptions.builder();
        builder.setCacheExpireMs(readableConfig.get(LOOKUP_CACHE_TTL).toMillis());
        builder.setCacheMaxSize(readableConfig.get(LOOKUP_CACHE_MAX_ROWS));
        builder.setMaxRetryTimes(readableConfig.get(LOOKUP_MAX_RETRIES));
        builder.setJdbcReadBatchSize(readableConfig.get(LOOKUP_JDBC_READ_BATCH_SIZE));
        builder.setJdbcReadBatchQueueSize(readableConfig.get(LOOKUP_JDBC_READ_BATCH_QUEUE_SIZE));
        builder.setJdbcReadThreadSize(readableConfig.get(LOOKUP_JDBC_READ_THREAD_SIZE));
        builder.setAsync(readableConfig.get(LOOKUP_JDBC_ASYNC));
        return builder.build();
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(
                        this, context);

        // validate all options
        helper.validateExcept(STREAM_LOAD_PROP_PREFIX);
        // sink parallelism
        final Integer parallelism = helper.getOptions().get(SINK_PARALLELISM);

        Properties streamLoadProp = getStreamLoadProp(context.getCatalogTable().getOptions());
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        // create and return dynamic table source
        return new StarRocksDynamicTableSink(
                getDorisOptions(helper.getOptions()),
                getDorisReadOptions(helper.getOptions()),
                getDorisExecutionOptions(helper.getOptions(), streamLoadProp),
                physicalSchema,
                parallelism
        );
    }
}

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
package com.nio.starRocks.flink.tools.cdc;

import com.nio.starRocks.flink.catalog.starRocks.StarRocksSystem;
import com.nio.starRocks.flink.catalog.starRocks.TableSchema;
import com.nio.starRocks.flink.cfg.StarRocksConnectionOptions;
import com.nio.starRocks.flink.cfg.StarRocksExecutionOptions;
import com.nio.starRocks.flink.cfg.StarRocksOptions;
import com.nio.starRocks.flink.cfg.StarRocksReadOptions;
import com.nio.starRocks.flink.sink.StarRocksSink;
import com.nio.starRocks.flink.sink.writer.JsonDebeziumSchemaSerializer;
import com.nio.starRocks.flink.table.StarRocksConfigOptions;
import com.nio.starRocks.flink.tools.cdc.mysql.ParsingProcessFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public abstract class DatabaseSync {
    private static final Logger LOG = LoggerFactory.getLogger(DatabaseSync.class);
    private static final String LIGHT_SCHEMA_CHANGE = "light_schema_change";
    private static final String TABLE_NAME_OPTIONS = "table-name";
    protected Configuration config;
    protected String database;
    protected TableNameConverter converter;
    protected Pattern includingPattern;
    protected Pattern excludingPattern;
    protected Map<String, String> tableConfig;
    protected Configuration sinkConfig;
    protected boolean ignoreDefaultValue;
    public StreamExecutionEnvironment env;
    private boolean createTableOnly = false;
    private boolean newSchemaChange;
    protected String includingTables;
    protected String excludingTables;

    public abstract Connection getConnection() throws SQLException;

    public abstract List<SourceSchema> getSchemaList() throws Exception;

    public abstract DataStreamSource<String> buildCdcSource(StreamExecutionEnvironment env);

    public void create(StreamExecutionEnvironment env, String database, Configuration config,
                       String tablePrefix, String tableSuffix, String includingTables,
                       String excludingTables, boolean ignoreDefaultValue, Configuration sinkConfig,
            Map<String, String> tableConfig, boolean createTableOnly, boolean useNewSchemaChange) {
        this.env = env;
        this.config = config;
        this.database = database;
        this.converter = new TableNameConverter(tablePrefix, tableSuffix);
        this.includingTables = includingTables;
        this.excludingTables = excludingTables;
        this.includingPattern = includingTables == null ? null : Pattern.compile(includingTables);
        this.excludingPattern = excludingTables == null ? null : Pattern.compile(excludingTables);
        this.ignoreDefaultValue = ignoreDefaultValue;
        this.sinkConfig = sinkConfig;
        this.tableConfig = tableConfig == null ? new HashMap<>() : tableConfig;
        //default enable light schema change
        if(!this.tableConfig.containsKey(LIGHT_SCHEMA_CHANGE)){
            this.tableConfig.put(LIGHT_SCHEMA_CHANGE, "true");
        }
        this.createTableOnly = createTableOnly;
        this.newSchemaChange = useNewSchemaChange;
    }

    public void build() throws Exception {
        StarRocksConnectionOptions options = getStarRocksConnectionOptions();
        StarRocksSystem starRocksSystem = new StarRocksSystem(options);

        List<SourceSchema> schemaList = getSchemaList();
        Preconditions.checkState(!schemaList.isEmpty(), "No tables to be synchronized.");
        if (!starRocksSystem.databaseExists(database)) {
            LOG.info("database {} not exist, created", database);
            starRocksSystem.createDatabase(database);
        }

        List<String> syncTables = new ArrayList<>();
        List<String> starRocksTables = new ArrayList<>();
        for (SourceSchema schema : schemaList) {
            syncTables.add(schema.getTableName());
            String starRocksTable = converter.convert(schema.getTableName());
            if (!starRocksSystem.tableExists(database, starRocksTable)) {
                TableSchema starRocksSchema = schema.convertTableSchema(tableConfig);
                //set starRocks target database
                starRocksSchema.setDatabase(database);
                starRocksSchema.setTable(starRocksTable);
                starRocksSystem.createTable(starRocksSchema);
            }
            starRocksTables.add(starRocksTable);
        }
        if(createTableOnly){
            System.out.println("Create table finished.");
            System.exit(0);
        }

        config.setString(TABLE_NAME_OPTIONS, "(" + String.join("|", syncTables) + ")");
        DataStreamSource<String> streamSource = buildCdcSource(env);
        SingleOutputStreamOperator<Void> parsedStream = streamSource.process(new ParsingProcessFunction(converter));
        for (String table : starRocksTables) {
            OutputTag<String> recordOutputTag = ParsingProcessFunction.createRecordOutputTag(table);
            DataStream<String> sideOutput = parsedStream.getSideOutput(recordOutputTag);

            int sinkParallel = sinkConfig.getInteger(StarRocksConfigOptions.SINK_PARALLELISM, sideOutput.getParallelism());
            sideOutput.sinkTo(buildStarRocksSink(table)).setParallelism(sinkParallel).name(table).uid(table);
        }
    }

    private StarRocksConnectionOptions getStarRocksConnectionOptions() {
        String fenodes = sinkConfig.getString(StarRocksConfigOptions.FENODES);
        String user = sinkConfig.getString(StarRocksConfigOptions.USERNAME);
        String passwd = sinkConfig.getString(StarRocksConfigOptions.PASSWORD, "");
        String jdbcUrl = sinkConfig.getString(StarRocksConfigOptions.JDBC_URL);
        Preconditions.checkNotNull(fenodes, "fenodes is empty in sink-conf");
        Preconditions.checkNotNull(user, "username is empty in sink-conf");
        Preconditions.checkNotNull(jdbcUrl, "jdbcurl is empty in sink-conf");
        StarRocksConnectionOptions.StarRocksConnectionOptionsBuilder builder = new StarRocksConnectionOptions.StarRocksConnectionOptionsBuilder()
                .withFenodes(fenodes)
                .withUsername(user)
                .withPassword(passwd)
                .withJdbcUrl(jdbcUrl);
        return builder.build();
    }

    /**
     * create starRocks sink
     */
    public StarRocksSink<String> buildStarRocksSink(String table) {
        String fenodes = sinkConfig.getString(StarRocksConfigOptions.FENODES);
        String benodes = sinkConfig.getString(StarRocksConfigOptions.BENODES);
        String user = sinkConfig.getString(StarRocksConfigOptions.USERNAME);
        String passwd = sinkConfig.getString(StarRocksConfigOptions.PASSWORD, "");
        String labelPrefix = sinkConfig.getString(StarRocksConfigOptions.SINK_LABEL_PREFIX);

        StarRocksSink.Builder<String> builder = StarRocksSink.builder();
        StarRocksOptions.Builder starRocksBuilder = StarRocksOptions.builder();
        starRocksBuilder.setFenodes(fenodes)
                .setBenodes(benodes)
                .setTableIdentifier(database + "." + table)
                .setUsername(user)
                .setPassword(passwd);

        Properties pro = new Properties();
        //default json data format
        pro.setProperty("format", "json");
        pro.setProperty("read_json_by_line", "true");
        //customer stream load properties
        Properties streamLoadProp = StarRocksConfigOptions.getStreamLoadProp(sinkConfig.toMap());
        pro.putAll(streamLoadProp);
        StarRocksExecutionOptions.Builder executionBuilder = StarRocksExecutionOptions.builder()
                .setLabelPrefix(String.join("-", labelPrefix, database, table))
                .setStreamLoadProp(pro);

        sinkConfig.getOptional(StarRocksConfigOptions.SINK_ENABLE_DELETE).ifPresent(executionBuilder::setDeletable);
        sinkConfig.getOptional(StarRocksConfigOptions.SINK_BUFFER_COUNT).ifPresent(executionBuilder::setBufferCount);
        sinkConfig.getOptional(StarRocksConfigOptions.SINK_BUFFER_SIZE).ifPresent(executionBuilder::setBufferSize);
        sinkConfig.getOptional(StarRocksConfigOptions.SINK_CHECK_INTERVAL).ifPresent(executionBuilder::setCheckInterval);
        sinkConfig.getOptional(StarRocksConfigOptions.SINK_MAX_RETRIES).ifPresent(executionBuilder::setMaxRetries);
        sinkConfig.getOptional(StarRocksConfigOptions.SINK_IGNORE_UPDATE_BEFORE).ifPresent(executionBuilder::setIgnoreUpdateBefore);

        boolean enable2pc = sinkConfig.getBoolean(StarRocksConfigOptions.SINK_ENABLE_2PC);
        if(!enable2pc){
            executionBuilder.disable2PC();
        }

        //batch option
        if(sinkConfig.getBoolean(StarRocksConfigOptions.SINK_ENABLE_BATCH_MODE)){
            executionBuilder.enableBatchMode();
        }
        sinkConfig.getOptional(StarRocksConfigOptions.SINK_FLUSH_QUEUE_SIZE).ifPresent(executionBuilder::setFlushQueueSize);
        sinkConfig.getOptional(StarRocksConfigOptions.SINK_BUFFER_FLUSH_MAX_ROWS).ifPresent(executionBuilder::setBufferFlushMaxRows);
        sinkConfig.getOptional(StarRocksConfigOptions.SINK_BUFFER_FLUSH_MAX_BYTES).ifPresent(executionBuilder::setBufferFlushMaxBytes);
        sinkConfig.getOptional(StarRocksConfigOptions.SINK_BUFFER_FLUSH_INTERVAL).ifPresent(v-> executionBuilder.setBufferFlushIntervalMs(v.toMillis()));

        StarRocksExecutionOptions executionOptions = executionBuilder.build();
        builder.setStarRocksReadOptions(StarRocksReadOptions.builder().build())
                .setStarRocksExecutionOptions(executionOptions)
                .setSerializer(JsonDebeziumSchemaSerializer.builder()
                        .setStarRocksOptions(starRocksBuilder.build())
                        .setNewSchemaChange(newSchemaChange)
                        .setExecutionOptions(executionOptions)
                        .build())
                .setStarRocksOptions(starRocksBuilder.build());
        return builder.build();
    }

    /**
     * Filter table that need to be synchronized
     */
    protected boolean isSyncNeeded(String tableName) {
        boolean sync = true;
        if (includingPattern != null) {
            sync = includingPattern.matcher(tableName).matches();
        }
        if (excludingPattern != null) {
            sync = sync && !excludingPattern.matcher(tableName).matches();
        }
        LOG.debug("table {} is synchronized? {}", tableName, sync);
        return sync;
    }

    public static class TableNameConverter implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String prefix;
        private final String suffix;

        TableNameConverter(){
            this("","");
        }

        TableNameConverter(String prefix, String suffix) {
            this.prefix = prefix == null ? "" : prefix;
            this.suffix = suffix == null ? "" : suffix;
        }

        public String convert(String tableName) {
            return prefix + tableName + suffix;
        }
    }
}

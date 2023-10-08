package com.nio.starRocks.flink.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.factories.FactoryUtil;


/**
 * 功能：
 * 作者：zhl
 * 日期:2023/9/21 15:02
 **/

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import static com.nio.starRocks.flink.cfg.ConfigurationOptions.STARROCKS_BATCH_SIZE_DEFAULT;
import static com.nio.starRocks.flink.cfg.ConfigurationOptions.STARROCKS_DESERIALIZE_ARROW_ASYNC_DEFAULT;
import static com.nio.starRocks.flink.cfg.ConfigurationOptions.STARROCKS_DESERIALIZE_QUEUE_SIZE_DEFAULT;
import static com.nio.starRocks.flink.cfg.ConfigurationOptions.STARROCKS_EXEC_MEM_LIMIT_DEFAULT;
import static com.nio.starRocks.flink.cfg.ConfigurationOptions.STARROCKS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT;
import static com.nio.starRocks.flink.cfg.ConfigurationOptions.STARROCKS_REQUEST_QUERY_TIMEOUT_S_DEFAULT;
import static com.nio.starRocks.flink.cfg.ConfigurationOptions.STARROCKS_REQUEST_READ_TIMEOUT_MS_DEFAULT;
import static com.nio.starRocks.flink.cfg.ConfigurationOptions.STARROCKS_REQUEST_RETRIES_DEFAULT;
import static com.nio.starRocks.flink.cfg.ConfigurationOptions.STARROCKS_TABLET_SIZE_DEFAULT;

public class StarRocksConfigOptions {
    public static final String IDENTIFIER = "starRocks";

    //TODO
    public static final ConfigOption<String> FENODES = ConfigOptions.key("fenodes").stringType().noDefaultValue().withDescription("starRocks fe http address.");
    public static final ConfigOption<String> BENODES = ConfigOptions.key("benodes").stringType().noDefaultValue().withDescription("starRocks be http address.");
    public static final ConfigOption<String> TABLE_IDENTIFIER = ConfigOptions.key("table.identifier").stringType().noDefaultValue().withDescription("the starRocks table name.");
    public static final ConfigOption<String> USERNAME = ConfigOptions.key("username").stringType().noDefaultValue().withDescription("the starRocks user name.");
    public static final ConfigOption<String> PASSWORD = ConfigOptions.key("password").stringType().noDefaultValue().withDescription("the starRocks password.");

    public static final ConfigOption<String> JDBC_URL = ConfigOptions.key("jdbc-url").stringType().noDefaultValue().withDescription("starRocks jdbc url address.");

    //source config options
    public static final ConfigOption<String>STARROCKS_READ_FIELD=ConfigOptions
            .key("starRocks.read.field")
            .stringType()
            .noDefaultValue()
            .withDescription("List of column names in the StarRocks table,separated by commas");
    public static final ConfigOption<String> STARRCOKS_FILTER_QUERY = ConfigOptions
            .key("starRocks.filter.query")
            .stringType()
            .noDefaultValue()
            .withDescription("Filter expression of the query, which is transparently transmitted to StarRocks. StarRocks uses this expression to complete source-side data filtering");
    public static final ConfigOption<Integer> STARROCKS_TABLET_SIZE = ConfigOptions
            .key("starRocks.request.tablet.size")
            .intType()
            .defaultValue(STARROCKS_TABLET_SIZE_DEFAULT)
            .withDescription("");
    public static final ConfigOption<Integer> STARROCKS_REQUEST_CONNECT_TIMEOUT_MS = ConfigOptions
            .key("starRocks.request.connect.timeout.ms")
            .intType()
            .defaultValue(STARROCKS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT)
            .withDescription("");
    public static final ConfigOption<Integer> STARROCKS_REQUEST_READ_TIMEOUT_MS = ConfigOptions
            .key("starRocks.request.read.timeout.ms")
            .intType()
            .defaultValue(STARROCKS_REQUEST_READ_TIMEOUT_MS_DEFAULT)
            .withDescription("");
    public static final ConfigOption<Integer> STARROCKS_REQUEST_QUERY_TIMEOUT_S = ConfigOptions
            .key("starRocks.request.query.timeout.s")
            .intType()
            .defaultValue(STARROCKS_REQUEST_QUERY_TIMEOUT_S_DEFAULT)
            .withDescription("");
    public static final ConfigOption<Integer> STARROCKS_REQUEST_RETRIES = ConfigOptions
            .key("starRocks.request.retries")
            .intType()
            .defaultValue(STARROCKS_REQUEST_RETRIES_DEFAULT)
            .withDescription("");
    public static final ConfigOption<String> STARROCKS_FILTER_QUERY = ConfigOptions
            .key("StarRocks.filter.query")
            .stringType()
            .noDefaultValue()
            .withDescription("Filter expression of the query, which is transparently transmitted to StarRocks. StarRocks uses this expression to complete source-side data filtering");
    public static final ConfigOption<Boolean> STARROCKS_DESERIALIZE_ARROW_ASYNC = ConfigOptions
            .key("starRocks.deserialize.arrow.async")
            .booleanType()
            .defaultValue(STARROCKS_DESERIALIZE_ARROW_ASYNC_DEFAULT)
            .withDescription("");
    public static final ConfigOption<Integer> STARROCKS_DESERIALIZE_QUEUE_SIZE = ConfigOptions
            .key("starRocks.request.retriesstarRocks.deserialize.queue.size")
            .intType()
            .defaultValue(STARROCKS_DESERIALIZE_QUEUE_SIZE_DEFAULT)
            .withDescription("");
    public static final ConfigOption<Integer> STARROCKS_BATCH_SIZE = ConfigOptions
            .key("starRocks.batch.size")
            .intType()
            .defaultValue(STARROCKS_BATCH_SIZE_DEFAULT)
            .withDescription("");
    public static final ConfigOption<Long> STARROCKS_EXEC_MEM_LIMIT = ConfigOptions
            .key("starRocks.exec.mem.limit")
            .longType()
            .defaultValue(STARROCKS_EXEC_MEM_LIMIT_DEFAULT)
            .withDescription("");
    public static final ConfigOption<Boolean> SOURCE_USE_OLD_API = ConfigOptions
            .key("source.use-old-api")
            .booleanType()
            .defaultValue(false)
            .withDescription("Whether to read data using the new interface defined according to the FLIP-27 specification,default false");
    // Lookup options
    public static final ConfigOption<Long> LOOKUP_CACHE_MAX_ROWS =
            ConfigOptions.key("lookup.cache.max-rows")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription(
                            "The max number of rows of lookup cache, over this value, the oldest rows will "
                                    + "be eliminated. \"cache.max-rows\" and \"cache.ttl\" options must all be specified if any of them is "
                                    + "specified.");

    public static final ConfigOption<Duration> LOOKUP_CACHE_TTL =
            ConfigOptions.key("lookup.cache.ttl")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription("The cache time to live.");

    public static final ConfigOption<Integer> LOOKUP_MAX_RETRIES =
            ConfigOptions.key("lookup.max-retries")
                    .intType()
                    .defaultValue(1)
                    .withDescription("The max retry times if lookup database failed.");

    public static final ConfigOption<Integer> LOOKUP_JDBC_READ_BATCH_SIZE =
            ConfigOptions.key("lookup.jdbc.read.batch.size")
                    .intType()
                    .defaultValue(128)
                    .withDescription("when dimension table query, save the maximum number of batches.");

    public static final ConfigOption<Integer> LOOKUP_JDBC_READ_BATCH_QUEUE_SIZE =
            ConfigOptions.key("lookup.jdbc.read.batch.queue-size")
                    .intType()
                    .defaultValue(256)
                    .withDescription("dimension table query request buffer queue size.");

    public static final ConfigOption<Integer> LOOKUP_JDBC_READ_THREAD_SIZE =
            ConfigOptions.key("lookup.jdbc.read.thread-size")
                    .intType()
                    .defaultValue(3)
                    .withDescription("the number of threads for dimension table query, each query occupies a JDBC connection");

    public static final ConfigOption<Boolean> LOOKUP_JDBC_ASYNC =
            ConfigOptions.key("lookup.jdbc.async")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("whether to set async lookup");


    // sink config options
    public static final ConfigOption<Boolean> SINK_ENABLE_2PC = ConfigOptions
            .key("sink.enable-2pc")
            .booleanType()
            .defaultValue(true)
            .withDescription("enable 2PC while loading");

    public static final ConfigOption<Integer> SINK_CHECK_INTERVAL = ConfigOptions
            .key("sink.check-interval")
            .intType()
            .defaultValue(10000)
            .withDescription("check exception with the interval while loading");
    public static final ConfigOption<Integer> SINK_MAX_RETRIES = ConfigOptions
            .key("sink.max-retries")
            .intType()
            .defaultValue(3)
            .withDescription("the max retry times if writing records to database failed.");
    public static final ConfigOption<Integer> SINK_BUFFER_SIZE = ConfigOptions
            .key("sink.buffer-size")
            .intType()
            .defaultValue(256 * 1024)
            .withDescription("the buffer size to cache data for stream load.");
    public static final ConfigOption<Integer> SINK_BUFFER_COUNT = ConfigOptions
            .key("sink.buffer-count")
            .intType()
            .defaultValue(3)
            .withDescription("the buffer count to cache data for stream load.");
    public static final ConfigOption<String> SINK_LABEL_PREFIX = ConfigOptions
            .key("sink.label-prefix")
            .stringType()
            .defaultValue("")
            .withDescription("the unique label prefix.");
    public static final ConfigOption<Boolean> SINK_ENABLE_DELETE = ConfigOptions
            .key("sink.enable-delete")
            .booleanType()
            .defaultValue(true)
            .withDescription("whether to enable the delete function");

    public static final ConfigOption<Integer> SINK_PARALLELISM = FactoryUtil.SINK_PARALLELISM;


    public static final ConfigOption<Boolean> SINK_ENABLE_BATCH_MODE = ConfigOptions
            .key("sink.enable.batch-mode")
            .booleanType()
            .defaultValue(false)
            .withDescription("Whether to enable batch write mode");

    public static final ConfigOption<Integer> SINK_FLUSH_QUEUE_SIZE = ConfigOptions
            .key("sink.flush.queue-size")
            .intType()
            .defaultValue(2)
            .withDescription("Queue length for async stream load, default is 2");

    public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS = ConfigOptions
            .key("sink.buffer-flush.max-rows")
            .intType()
            .defaultValue(50000)
            .withDescription("The maximum number of flush items in each batch, the default is 5w");

    public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_BYTES = ConfigOptions
            .key("sink.buffer-flush.max-bytes")
            .intType()
            .defaultValue(10 * 1024 * 1024)
            .withDescription("The maximum number of bytes flushed in each batch, the default is 10MB");

    public static final ConfigOption<Duration> SINK_BUFFER_FLUSH_INTERVAL = ConfigOptions
            .key("sink.buffer-flush.interval")
            .durationType()
            .defaultValue(Duration.ofSeconds(10))
            .withDescription("the flush interval mills, over this time, asynchronous threads will flush data. The " +
                    "default value is 10s.");

    public static final ConfigOption<Boolean> SINK_IGNORE_UPDATE_BEFORE = ConfigOptions
            .key("sink.ignore.update-before")
            .booleanType()
            .defaultValue(true)
            .withDescription("In the CDC scenario, when the primary key of the upstream is inconsistent with that of the downstream, the update-before data needs to be passed to the downstream as deleted data, otherwise the data cannot be deleted.\n" +
                    "The default is to ignore, that is, perform upsert semantics.");

    // Prefix for StarRocks StreamLoad specific properties.
    public static final String STREAM_LOAD_PROP_PREFIX = "sink.properties.";

    public static Properties getStreamLoadProp(Map<String, String> tableOptions) {
        final Properties streamLoadProp = new Properties();

        for (Map.Entry<String, String> entry : tableOptions.entrySet()) {
            if (entry.getKey().startsWith(STREAM_LOAD_PROP_PREFIX)) {
                String subKey = entry.getKey().substring(STREAM_LOAD_PROP_PREFIX.length());
                streamLoadProp.put(subKey, entry.getValue());
            }
        }
        return streamLoadProp;
    }
}

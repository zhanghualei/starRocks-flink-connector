package com.nio.starRocks.flink.cfg;

/**
 * 功能：
 * 作者：zhl
 * 日期:2023/9/21 17:47
 **/
public interface ConfigurationOptions {

    String STARROCKS_FENODES = "fenodes";

    String STARROCKS_DEFAULT_CLUSTER = "default_cluster";

    String TABLE_IDENTIFIER = "table.identifier";
    String STARROCKS_TABLE_IDENTIFIER = "starRocks.table.identifier";
    String STARROCKS_READ_FIELD = "starRocks.read.field";
    String STARROCKS_FILTER_QUERY = "starRocks.filter.query";
    String STARROCKS_FILTER_QUERY_IN_MAX_COUNT = "starRocks.filter.query.in.max.count";
    Integer STARROCKS_FILTER_QUERY_IN_VALUE_UPPER_LIMIT = 10000;

    String STARROCKS_USER = "username";
    String STARROCKS_PASSWORD = "password";

    String STARROCKS_REQUEST_AUTH_USER = "starRocks.request.auth.user";
    String STARROCKS_REQUEST_AUTH_PASSWORD = "starRocks.request.auth.password";
    String STARROCKS_REQUEST_RETRIES = "starRocks.request.retries";
    String STARROCKS_REQUEST_CONNECT_TIMEOUT_MS = "starRocks.request.connect.timeout.ms";
    String STARROCKS_REQUEST_READ_TIMEOUT_MS = "starRocks.request.read.timeout.ms";
    String STARROCKS_REQUEST_QUERY_TIMEOUT_S = "starRocks.request.query.timeout.s";
    Integer STARROCKS_REQUEST_RETRIES_DEFAULT = 3;
    Integer STARROCKS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT = 30 * 1000;
    Integer STARROCKS_REQUEST_READ_TIMEOUT_MS_DEFAULT = 30 * 1000;
    Integer STARROCKS_REQUEST_QUERY_TIMEOUT_S_DEFAULT = 3600;

    String STARROCKS_TABLET_SIZE = "starRocks.request.tablet.size";
    Integer STARROCKS_TABLET_SIZE_DEFAULT = Integer.MAX_VALUE;
    Integer STARROCKS_TABLET_SIZE_MIN = 1;

    String STARROCKS_BATCH_SIZE = "starRocks.batch.size";
    Integer STARROCKS_BATCH_SIZE_DEFAULT = 1024;

    String STARROCKS_EXEC_MEM_LIMIT = "starRocks.exec.mem.limit";
    Long STARROCKS_EXEC_MEM_LIMIT_DEFAULT = 2147483648L;

    String STARROCKS_VALUE_READER_CLASS = "starRocks.value.reader.class";

    String STARROCKS_DESERIALIZE_ARROW_ASYNC = "starRocks.deserialize.arrow.async";
    Boolean STARROCKS_DESERIALIZE_ARROW_ASYNC_DEFAULT = false;

    String STARROCKS_DESERIALIZE_QUEUE_SIZE = "starRocks.deserialize.queue.size";
    Integer STARROCKS_DESERIALIZE_QUEUE_SIZE_DEFAULT = 64;

}

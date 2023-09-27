package com.nio.starRocks.flink.catalog;

import com.nio.starRocks.flink.cfg.StarRocksConnectionOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

import static com.nio.starRocks.flink.catalog.StarRocksCatalogOptions.DEFAULT_DATABASE;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.JDBC_URL;
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
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.PASSWORD;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.SINK_BUFFER_COUNT;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.SINK_BUFFER_SIZE;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.SINK_CHECK_INTERVAL;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.SINK_ENABLE_2PC;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.SINK_ENABLE_DELETE;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.SINK_LABEL_PREFIX;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.SINK_MAX_RETRIES;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.SINK_PARALLELISM;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.SOURCE_USE_OLD_API;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.STREAM_LOAD_PROP_PREFIX;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.TABLE_IDENTIFIER;
import static com.nio.starRocks.flink.table.StarRocksConfigOptions.USERNAME;

/**
 * 功能：
 * 作者：zhl
 * 日期:2023/9/21 17:30
 **/
public class StarRocksCatalogFactory implements CatalogFactory {

    @Override
    public String factoryIdentifier(){
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>>requiredOptions(){
        final Set<ConfigOption<?>> options=new HashSet<>();
        options.add(JDBC_URL);
        options.add(USERNAME);
        options.add(PASSWORD);
        return options;
    }

    @Override
    public Set<ConfigOption<?>>optionalOptions(){
        final Set<ConfigOption<?>>options=new HashSet<>();
        options.add(JDBC_URL);
        options.add(DEFAULT_DATABASE);

        options.add(FENODES);
        options.add(TABLE_IDENTIFIER);
        options.add(USERNAME);
        options.add(PASSWORD);

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

        options.add(SINK_CHECK_INTERVAL);
        options.add(SINK_ENABLE_2PC);
        options.add(SINK_MAX_RETRIES);
        options.add(SINK_ENABLE_DELETE);
        options.add(SINK_LABEL_PREFIX);
        options.add(SINK_BUFFER_SIZE);
        options.add(SINK_BUFFER_COUNT);
        options.add(SINK_PARALLELISM);

        options.add(SOURCE_USE_OLD_API);
        return options;
    }
    @Override
    public Catalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validateExcept(STREAM_LOAD_PROP_PREFIX);

        StarRocksConnectionOptions connectionOptions =
                new StarRocksConnectionOptions.StarRocksConnectionOptionsBuilder()
                        .withFenodes(helper.getOptions().get(FENODES))
                        .withJdbcUrl(helper.getOptions().get(JDBC_URL))
                        .withUsername(helper.getOptions().get(USERNAME))
                        .withPassword(helper.getOptions().get(PASSWORD))
                        .build();
        return new StarRocksCatalog(
                context.getName(),
                connectionOptions,
                helper.getOptions().get(DEFAULT_DATABASE),
                ((Configuration) helper.getOptions()).toMap());
    }
}

package com.nio.starRocks.flink.catalog;

import com.nio.starRocks.flink.cfg.StarRocksConnectionOptions;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CollectionUtil;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * 功能：
 * 作者：zhl
 * 日期:2023/9/21 16:50
 **/
@Ignore
public class CatalogTest {
    private static final String TEST_CATALOG_NAME = "starRocks_catalog";
    private static final String TEST_FENODES = "10.129.88.144:8030";
    private static final String TEST_JDBCURL = "jdbc:mysql://10.129.88.144:9030";
    private static final String TEST_USERNAME = "data_infra";
    private static final String TEST_PWD = "starrocks_test_gZ2|eO";
    private static final String TEST_DB = "data_infra_flink_connector_test";
    private static final String TEST_TABLE = "t_all_types";
    private static final String TEST_TABLE_SINK = "t_all_types_sink";
    private static final String TEST_TABLE_SINK_GROUPBY = "t_all_types_sink_groupby";

    protected static final Schema TABLE_SCHEMA=
            Schema.newBuilder()
                    .column("id", DataTypes.STRING())
                    .column("c_boolean",DataTypes.BOOLEAN())
                    .column("c_char", DataTypes.CHAR(1))
                    .column("c_date", DataTypes.DATE())
                    .column("c_datetime", DataTypes.TIMESTAMP(0))
                    .column("c_decimal", DataTypes.DECIMAL(10, 2))
                    .column("c_double", DataTypes.DOUBLE())
                    .column("c_float", DataTypes.FLOAT())
                    .column("c_int", DataTypes.INT())
                    .column("c_bigint", DataTypes.BIGINT())
                    .column("c_largeint", DataTypes.STRING())
                    .column("c_smallint", DataTypes.SMALLINT())
                    .column("c_string", DataTypes.STRING())
                    .column("c_tinyint", DataTypes.TINYINT())
                    .build();
    protected static final TableSchema TABLE_SCHEMA_1=
            TableSchema.builder()
                    .field("id",new AtomicDataType(new VarCharType(false,128)))
                    .field("c_boolean", DataTypes.BOOLEAN())
                    .field("c_char", DataTypes.CHAR(1))
                    .field("c_date", DataTypes.DATE())
                    .field("c_datetime", DataTypes.TIMESTAMP(0))
                    .field("c_decimal", DataTypes.DECIMAL(10, 2))
                    .field("c_double", DataTypes.DOUBLE())
                    .field("c_float", DataTypes.FLOAT())
                    .field("c_int", DataTypes.INT())
                    .field("c_bigint", DataTypes.BIGINT())
                    .field("c_largeint", DataTypes.STRING())
                    .field("c_smallint", DataTypes.SMALLINT())
                    .field("c_string", DataTypes.STRING())
                    .field("c_tinyint", DataTypes.TINYINT())
                    .primaryKey("id")
                    .build();

    private static final List<Row> ALL_TYPES_ROWS =
            Lists.newArrayList(
                    Row.ofKind(
                            RowKind.INSERT,
                            "100001",
                            true,
                            "a",
                            Date.valueOf("2022-08-31").toLocalDate(),
                            Timestamp.valueOf("2022-08-31 11:12:13").toLocalDateTime(),
                            BigDecimal.valueOf(1.12).setScale(2),
                            1.1234d,
                            1.1f,
                            1234567,
                            1234567890L,
                            "123456790123456790",
                            Short.parseShort("10"),
                            "catalog",
                            Byte.parseByte("1")),
                    Row.ofKind(
                            RowKind.INSERT,
                            "100002",
                            true,
                            "a",
                            Date.valueOf("2022-08-31").toLocalDate(),
                            Timestamp.valueOf("2022-08-31 11:12:13").toLocalDateTime(),
                            BigDecimal.valueOf(1.12).setScale(2),
                            1.1234d,
                            1.1f,
                            1234567,
                            1234567890L,
                            "123456790123456790",
                            Short.parseShort("10"),
                            "catalog",
                            Byte.parseByte("1")));

    private StarRocksCatalog catalog;
    private TableEnvironment tEnv;

    @Before
    public void setup() {
        StarRocksConnectionOptions connectionOptions =
                new StarRocksConnectionOptions.StarRocksConnectionOptionsBuilder()
                        .withFenodes(TEST_FENODES)
                        .withJdbcUrl(TEST_JDBCURL)
                        .withUsername(TEST_USERNAME)
                        .withPassword(TEST_PWD)
                        .build();

        Map<String,String> props = new HashMap<>();
        props.put("sink.enable-2pc","false");
        catalog = new StarRocksCatalog(TEST_CATALOG_NAME, connectionOptions, TEST_DB, props);
        this.tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.getConfig().set(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        // Use StarRocks catalog.
        tEnv.registerCatalog(TEST_CATALOG_NAME, catalog);
        tEnv.useCatalog(TEST_CATALOG_NAME);
    }

    /**
     * Caused by: java.sql.SQLSyntaxErrorException: Access denied; you need (at least one of) the
     *            ADMIN/OPERATOR privilege(s) for this operation
     */
//    @Test
//    public void testQueryFenodes(){
//        String actual = catalog.queryFenodes();
//        assertEquals("127.0.0.1:8030", actual);
//    }

    /**
     * Set the current default catalog as [starRocks_catalog] and the current default database as
     * [data_infra_flink_connector_test].
     */
    @Test
    public void testListDatabases() {
        List<String> actual = catalog.listDatabases();
        assertEquals(Collections.singletonList(TEST_DB), actual);
    }

    /**
     * Set the current default catalog as [starRocks_catalog] and the current default database as
     * [data_infra_flink_connector_test].
     * @throws Exception
     */
    @Test
    public void testDbExists() throws Exception {
        String databaseNotExist = "nonexistent";
        assertFalse(catalog.databaseExists(databaseNotExist));
        assertTrue(catalog.databaseExists(TEST_DB));
    }

    /**
     * Caused by: java.sql.SQLSyntaxErrorException: Access denied for user 'data_infra' to database 'db1'
     *
     * SQL query could not be executed :CREATE DATABASE IF NOT EXISTS db1
     * 但是：换成data_infra_flink_connector_test数据库就不会报错
     * @throws Exception
     */
    @Test
    public void testCreateDb() throws Exception {
        catalog.createDatabase("db1",createDb(), true);
        assertTrue(catalog.databaseExists("db1"));
    }
    private static CatalogDatabase createDb() {
        return new CatalogDatabaseImpl(
                new HashMap<String, String>() {
                    {
                        put("k1", "v1");
                    }
                },
                "");
    }

    /**
     * 不支持:没有其他有权限的库---暂时不测试
     */
    @Test
    public void testDropDb() throws Exception {
        catalog.dropDatabase("db1",false);
        assertFalse(catalog.databaseExists("db1"));
    }

    /**
     * 成功
     * @throws DatabaseNotExistException
     */
    @Test
    public void testListTables() throws DatabaseNotExistException {
        List<String> actual = catalog.listTables(TEST_DB);
//        assertEquals(
//                Arrays.asList(
//                        TEST_TABLE,
//                        TEST_TABLE_SINK,
//                        TEST_TABLE_SINK_GROUPBY),
//                actual);
    }

    /**
     * 成功
     */
    @Test
    public void testTableExists() {
        String tableNotExist = "nonExist";
        assertFalse(catalog.tableExists(new ObjectPath(TEST_DB, tableNotExist)));
    }


    /**
     * 成功，但是定义的类型和库中创建的表的类型之间存在差异
     * @throws TableNotExistException
     * @throws org.apache.flink.table.catalog.exceptions.TableNotExistException
     */
    @Test
    @Ignore
    public void testGetTable() throws TableNotExistException {
        //todo: string varchar mapping
        CatalogBaseTable table = catalog.getTable(new ObjectPath(TEST_DB, TEST_TABLE));
        System.out.println(table);
        assertEquals(TABLE_SCHEMA, table.getUnresolvedSchema());
    }



    //暂时还有问题??????????????????????
    @Test
    @Ignore
    public void testCreateTable() throws TableNotExistException, TableAlreadyExistException,
            DatabaseNotExistException {
        //todo: Record primary key not null information
        ObjectPath tablePath = new ObjectPath(TEST_DB, TEST_TABLE);

//        System.out.println(tablePath);

        catalog.dropTable(tablePath, true);
        catalog.createTable(tablePath, createTable(), true);

        CatalogBaseTable tableGet = catalog.getTable(tablePath);
        System.out.println(tableGet.getUnresolvedSchema());
        System.out.println(TABLE_SCHEMA_1);
        assertEquals(TABLE_SCHEMA_1, tableGet.getUnresolvedSchema());
    }

    private static CatalogTable createTable() {
        return new CatalogTableImpl(
                TABLE_SCHEMA_1,
                new HashMap<String, String>() {
                    {
                        put("connector", "starRocks");
                        put("table.properties.replication_num", "1");
                    }
                },
                "FlinkTable");
    }

    /**成功
     * @throws TableNotExistException
     */
    @Test
    public void testDropTable() throws TableNotExistException {
        catalog.dropTable(new ObjectPath("data_infra_flink_connector_test", "t_all_types"), true);
        assertFalse(catalog.tableExists(new ObjectPath("data_infra_flink_connector_test", "t_all_types")));
    }


    /**
     *暂时有问题?????????????????????????
     */
    @Test
    public void testSelectField() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(String.format("select id from %s", TEST_TABLE))
                                .execute()
                                .collect());
        assertEquals(
                Lists.newArrayList(Row.ofKind(RowKind.INSERT, "100001"),
                        Row.ofKind(RowKind.INSERT, "100002")), results);
    }

    /**
     * 暂时不支持,有问题？？？？？？？？？？？？？
     */
    @Test
    public void testWithoutCatalogDB() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(String.format("select * from %s", TEST_TABLE))
                                .execute()
                                .collect());
        assertEquals(ALL_TYPES_ROWS, results);
    }

    @Test
    public void testWithoutCatalog() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                String.format(
                                        "select * from `%s`.`%s`",
                                        TEST_DB, TEST_TABLE))
                                .execute()
                                .collect());
        assertEquals(ALL_TYPES_ROWS, results);
    }

    @Test
    public void testFullPath() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                String.format(
                                        "select * from %s.%s.`%s`",
                                        TEST_CATALOG_NAME,
                                        catalog.getDefaultDatabase(),
                                        TEST_TABLE))
                                .execute()
                                .collect());
        assertEquals(ALL_TYPES_ROWS, results);
    }

    @Test
    public void testSelectToInsert() throws Exception {

        String sql =
                String.format(
                        "insert into `%s` select * from `%s`",
                        TEST_TABLE_SINK, TEST_TABLE);
        tEnv.executeSql(sql).await();

        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(String.format("select * from %s", TEST_TABLE_SINK))
                                .execute()
                                .collect());
        assertEquals(ALL_TYPES_ROWS, results);
    }

    @Test
    public void testGroupByInsert() throws Exception {
        // Changes primary key for the next record.
        tEnv.executeSql(
                String.format(
                        "insert into `%s` select  `c_string`, max(`id`) `id` from `%s` "
                                + "group by `c_string` ",
                        TEST_TABLE_SINK_GROUPBY, TEST_TABLE))
                .await();

        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                String.format(
                                        "select * from `%s`",
                                        TEST_TABLE_SINK_GROUPBY))
                                .execute()
                                .collect());
        assertEquals(Lists.newArrayList(Row.ofKind(RowKind.INSERT, "catalog","100002")), results);
    }

}

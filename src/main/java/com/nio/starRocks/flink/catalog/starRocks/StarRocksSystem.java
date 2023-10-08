package com.nio.starRocks.flink.catalog.starRocks;


/**
 * 功能：
 * 作者：zhl
 * 日期:2023/9/21 11:12
 **/


import org.apache.commons.compress.utils.Lists;
import com.nio.starRocks.flink.cfg.StarRocksConnectionOptions;
import com.nio.starRocks.flink.connection.JdbcConnectionProvider;
import com.nio.starRocks.flink.connection.SimpleJdbcConnectionProvider;
import com.nio.starRocks.flink.exception.CreateTableException;
import com.nio.starRocks.flink.exception.StarRocksRuntimeException;
import com.nio.starRocks.flink.exception.StarRocksSystemException;
import com.nio.starRocks.flink.tools.cdc.DatabaseSync;
import org.apache.flink.annotation.Public;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * StarRocks System Operate
 */
@Public
public class StarRocksSystem {
    private static final Logger LOG = LoggerFactory.getLogger(DatabaseSync.class);
    private JdbcConnectionProvider jdbcConnectionProvider;
    private static final List<String> builtinDatabases = Arrays.asList("information_schema");

    public StarRocksSystem(StarRocksConnectionOptions options) {
        this.jdbcConnectionProvider = new SimpleJdbcConnectionProvider(options);
    }

    public List<String> listDatabases() {
        return extractColumnValuesBySQL(
                "SELECT `SCHEMA_NAME` FROM `INFORMATION_SCHEMA`.`SCHEMATA`;",
                1,
                dbName -> !builtinDatabases.contains(dbName));
    }

    public boolean databaseExists(String database) {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(database));
        return listDatabases().contains(database);
    }

    public boolean createDatabase(String database) {
        execute(String.format("CREATE DATABASE IF NOT EXISTS %s", database));
        return true;
    }

    public boolean dropDatabase(String database) {
        execute(String.format("DROP DATABASE IF EXISTS %s", database));
        return true;
    }

    public boolean tableExists(String database, String table){
        return databaseExists(database)
                && listTables(database).contains(table);
    }

    public List<String> listTables(String databaseName) {
        if (!databaseExists(databaseName)) {
            throw new StarRocksRuntimeException("database" + databaseName + " is not exists");
        }
        return extractColumnValuesBySQL(
                "SELECT TABLE_NAME FROM information_schema.`TABLES` WHERE TABLE_SCHEMA = ?",
                1,
                null,
                databaseName);
    }

    public void dropTable(String tableName) {
        execute(String.format("DROP TABLE IF EXISTS %s", tableName));
    }

    public void createTable(TableSchema schema) {
        String ddl = buildCreateTableDDL(schema);
        LOG.info("Create table with ddl:{}", ddl);
        execute(ddl);
    }

    public void execute(String sql) {
        try (Statement statement = jdbcConnectionProvider.getOrEstablishConnection().createStatement()) {
            statement.execute(sql);
        } catch (Exception e){
            throw new StarRocksSystemException(String.format("SQL query could not be executed: %s", sql), e);
        }
    }

    public List<String> extractColumnValuesBySQL(
            String sql,
            int columnIndex,
            Predicate<String> filterFunc,
            Object... params) {

        List<String> columnValues = Lists.newArrayList();
        try (PreparedStatement ps = jdbcConnectionProvider.getOrEstablishConnection().prepareStatement(sql)) {
            if (Objects.nonNull(params) && params.length > 0) {
                for (int i = 0; i < params.length; i++) {
                    ps.setObject(i + 1, params[i]);
                }
            }
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String columnValue = rs.getString(columnIndex);
                if (Objects.isNull(filterFunc) || filterFunc.test(columnValue)) {
                    columnValues.add(columnValue);
                }
            }
            return columnValues;
        } catch (Exception e) {
            throw new StarRocksSystemException(
                    String.format(
                            "The following SQL query could not be executed: %s", sql),
                    e);
        }
    }

    /**
     * 该方法是创建一个主键模型表
     * @param schema
     * @return
     */
    public String buildCreateTableDDL(TableSchema schema) {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        //sb最终的结果为`db`.`tb`(
        sb.append(identifier(schema.getDatabase()))
                .append(".")
                .append(identifier(schema.getTable()))
                .append("(");

        Map<String, FieldSchema> fields = schema.getFields();
        List<String> keys = schema.getKeys();

        for (String key : keys) {
            System.out.println(key);
        }


        //append keys
        for(String key : keys){
            if(!fields.containsKey(key)){
                throw new CreateTableException("key " + key + " not found in column list");
            }
            FieldSchema field = fields.get(key);
            buildColumn(sb, field, true);
        }

        //append values
        for (Map.Entry<String, FieldSchema> entry : fields.entrySet()) {
            if(keys.contains(entry.getKey())){
                continue;
            }
            FieldSchema field = entry.getValue();
            buildColumn(sb, field, false);

        }
        sb = sb.deleteCharAt(sb.length() -1);
        sb.append(" ) ");
        //append uniq model
        if(DataModel.UNIQUE.equals(schema.getModel())){
            sb.append(schema.getModel().name())
                    .append(" KEY(")
                    .append(String.join(",", identifier(schema.getKeys())))
                    .append(")");
        }

        //append table comment
        if(!StringUtils.isNullOrWhitespaceOnly(schema.getTableComment())){
            sb.append(" COMMENT '")
                    .append(schema.getTableComment())
                    .append("' ");
        }

        //append distribute key
        sb.append(" DISTRIBUTED BY HASH(")
                .append(String.join(",", identifier(schema.getDistributeKeys())))
                .append(") BUCKETS 8 ");//此处将创建表时候的桶的数量限定为2,也可以通过传递参数进行修改

        //append properties
        int index = 0;
        for (Map.Entry<String, String> entry : schema.getProperties().entrySet()) {
            if (index == 0) {
                sb.append(" PROPERTIES (");
            }
            if (index > 0) {
                sb.append(",");
            }
            sb.append(quoteProperties(entry.getKey()))
                    .append("=")
                    .append(quoteProperties(entry.getValue()));
            index++;

            if (index == schema.getProperties().size()) {
                sb.append(")");
            }
        }
        return sb.toString();
    }

    private void buildColumn(StringBuilder sql, FieldSchema field, boolean isKey){
        String fieldType = field.getTypeString();
        if(isKey && StarRocksType.STRING.equals(fieldType)){
            fieldType = String.format("%s(%s)", StarRocksType.VARCHAR, 65533);
        }
        sql.append(identifier(field.getName()))
                .append(" ")
                .append(fieldType)
                .append(" COMMENT '")
                .append(quoteComment(field.getComment()))
                .append("',");
    }

    private String quoteComment(String comment){
        if(comment == null){
            return "";
        } else {
            return comment.replaceAll("'","\\\\'");
        }
    }

    private List<String> identifier(List<String> name) {
        List<String> result = name.stream().map(m -> identifier(m)).collect(Collectors.toList());
        return result;
    }

    private String identifier(String name) {
        return "`" + name + "`";
    }

    private String quoteProperties(String name) {
        return "'" + name + "'";
    }

}


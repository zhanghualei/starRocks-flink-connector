package com.nio.starRocks.flink.catalog;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.CommonCatalogOptions;

import java.util.HashMap;
import java.util.Map;

/**
 * 功能：
 * 作者：zhl
 * 日期:2023/9/21 15:35
 **/
public class StarRocksCatalogOptions {

    public static final ConfigOption<String> DEFAULT_DATABASE = ConfigOptions.key(CommonCatalogOptions.DEFAULT_DATABASE_KEY).stringType().noDefaultValue();

    public static final String TABLE_PROPERTIES_PREFIX="table.properties.";

    public static Map<String,String>getCreateTableProps(Map<String,String> tableOptions){
        final Map<String,String> tableProps=new HashMap<>();

        for (Map.Entry<String, String> entry : tableOptions.entrySet()) {
            if(entry.getKey().startsWith(TABLE_PROPERTIES_PREFIX)){
                String subKey = entry.getKey().substring(TABLE_PROPERTIES_PREFIX.length());
                tableProps.put(subKey,entry.getValue());
            }
        }
        return tableProps;
    }
}

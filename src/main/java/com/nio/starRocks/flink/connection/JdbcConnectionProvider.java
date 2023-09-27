package com.nio.starRocks.flink.connection;

import java.sql.Connection;

/**
 * 功能：
 * 作者：zhl
 * 日期:2023/9/21 11:26
 **/
public interface JdbcConnectionProvider {

    /**
     * Get existing connection or establish an new one if there is none.
     */

    Connection getOrEstablishConnection() throws Exception;

    /** Close possible existing connection. */
    void closeConnection();

}

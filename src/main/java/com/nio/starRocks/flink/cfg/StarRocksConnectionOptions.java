package com.nio.starRocks.flink.cfg;

import java.io.Serializable;
import org.apache.flink.util.Preconditions;

/**
 * 功能：StarRocks connection options
 * 作者：zhl
 * 日期:2023/9/21 11:15
 **/

public class StarRocksConnectionOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    protected final String fenodes;
    protected final String username;
    protected final String password;
    protected String jdbcUrl;
    protected String benodes;

    public StarRocksConnectionOptions(String fenodes, String username, String password) {
        this.fenodes = Preconditions.checkNotNull(fenodes, "fenodes  is empty");
        this.username = username;
        this.password = password;
    }

    public StarRocksConnectionOptions(String fenodes, String username, String password, String jdbcUrl) {
        this(fenodes, username, password);
        this.jdbcUrl = jdbcUrl;
    }

    public StarRocksConnectionOptions(String fenodes, String benodes,  String username, String password,
                                  String jdbcUrl) {
        this(fenodes, username, password);
        this.benodes = benodes;
        this.jdbcUrl = jdbcUrl;
    }

    public String getFenodes() {
        return fenodes;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getBenodes() {
        return benodes;
    }

    public String getJdbcUrl(){
        return jdbcUrl;
    }

    /**
     * Builder for {@link StarRocksConnectionOptions}.
     */
    public static class StarRocksConnectionOptionsBuilder {
        private String fenodes;
        private String username;
        private String password;

        private String jdbcUrl;

        public StarRocksConnectionOptionsBuilder withFenodes(String fenodes) {
            this.fenodes = fenodes;
            return this;
        }

        public StarRocksConnectionOptionsBuilder withUsername(String username) {
            this.username = username;
            return this;
        }

        public StarRocksConnectionOptionsBuilder withPassword(String password) {
            this.password = password;
            return this;
        }

        public StarRocksConnectionOptionsBuilder withJdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
            return this;
        }

        public StarRocksConnectionOptions build() {
            return new StarRocksConnectionOptions(fenodes, username, password, jdbcUrl);
        }
    }

}

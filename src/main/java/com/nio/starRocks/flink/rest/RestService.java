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

package com.nio.starRocks.flink.rest;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import com.nio.starRocks.flink.cfg.ConfigurationOptions;
import com.nio.starRocks.flink.cfg.StarRocksOptions;
import com.nio.starRocks.flink.cfg.StarRocksReadOptions;
import com.nio.starRocks.flink.exception.ConnectedFailedException;
import com.nio.starRocks.flink.exception.StarRocksException;
import com.nio.starRocks.flink.exception.StarRocksRuntimeException;
import com.nio.starRocks.flink.exception.IllegalArgumentException;
import com.nio.starRocks.flink.exception.ShouldNeverHappenException;
import com.nio.starRocks.flink.rest.models.Backend;
import com.nio.starRocks.flink.rest.models.BackendRow;
import com.nio.starRocks.flink.rest.models.BackendV2;
import com.nio.starRocks.flink.rest.models.QueryPlan;
import com.nio.starRocks.flink.rest.models.Schema;
import com.nio.starRocks.flink.rest.models.Tablet;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;

import static com.nio.starRocks.flink.cfg.ConfigurationOptions.STARROCKS_TABLET_SIZE;
import static com.nio.starRocks.flink.cfg.ConfigurationOptions.STARROCKS_TABLET_SIZE_DEFAULT;
import static com.nio.starRocks.flink.cfg.ConfigurationOptions.STARROCKS_TABLET_SIZE_MIN;
import static com.nio.starRocks.flink.util.ErrorMessages.CONNECT_FAILED_MESSAGE;
import static com.nio.starRocks.flink.util.ErrorMessages.ILLEGAL_ARGUMENT_MESSAGE;
import static com.nio.starRocks.flink.util.ErrorMessages.SHOULD_NOT_HAPPEN_MESSAGE;


/**
 * Service for communicate with Doris FE.
 */
public class RestService implements Serializable {
    public final static int REST_RESPONSE_STATUS_OK = 200;
    public final static int REST_RESPONSE_CODE_OK = 0;
    private final static String REST_RESPONSE_BE_ROWS_KEY = "rows";
    private static final String API_PREFIX = "/api";
    private static final String SCHEMA = "_schema";
    private static final String QUERY_PLAN = "_query_plan";
    private static final String UNIQUE_KEYS_TYPE = "UNIQUE_KEYS";
    @Deprecated
    private static final String BACKENDS = "/rest/v1/system?path=//backends";
    private static final String BACKENDS_V2 = "/api/backends?is_alive=true";
    private static final String FE_LOGIN = "/rest/v1/login";

    /**
     * send request to Doris FE and get response json string.
     *
     * @param options configuration of request
     * @param request {@link HttpRequestBase} real request
     * @param logger  {@link Logger}
     * @return Doris FE response in json string
     * @throws ConnectedFailedException throw when cannot connect to Doris FE
     */
    private static String send(StarRocksOptions options, StarRocksReadOptions readOptions, HttpRequestBase request, Logger logger) throws
            ConnectedFailedException {
        int connectTimeout = readOptions.getRequestConnectTimeoutMs() == null ? ConfigurationOptions.STARROCKS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT : readOptions.getRequestConnectTimeoutMs();
        int socketTimeout = readOptions.getRequestReadTimeoutMs() == null ? ConfigurationOptions.STARROCKS_REQUEST_READ_TIMEOUT_MS_DEFAULT : readOptions.getRequestReadTimeoutMs();
        int retries = readOptions.getRequestRetries() == null ? ConfigurationOptions.STARROCKS_REQUEST_RETRIES_DEFAULT : readOptions.getRequestRetries();
        logger.trace("connect timeout set to '{}'. socket timeout set to '{}'. retries set to '{}'.",
                connectTimeout, socketTimeout, retries);

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(connectTimeout)
                .setSocketTimeout(socketTimeout)
                .build();

        request.setConfig(requestConfig);
        logger.info("Send request to Doris FE '{}' with user '{}'.", request.getURI(), options.getUsername());
        IOException ex = null;
        int statusCode = -1;

        for (int attempt = 0; attempt < retries; attempt++) {
            logger.debug("Attempt {} to request {}.", attempt, request.getURI());
            try {
                String response;
                if (request instanceof HttpGet) {
                    response = getConnectionGet(request, options.getUsername(), options.getPassword(), logger);
                } else {
                    response = getConnectionPost(request, options.getUsername(), options.getPassword(), logger);
                }
                if (response == null) {
                    logger.warn("Failed to get response from Doris FE {}, http code is {}",
                            request.getURI(), statusCode);
                    continue;
                }
                logger.trace("Success get response from Doris FE: {}, response is: {}.",
                        request.getURI(), response);
                //Handle the problem of inconsistent data format returned by http v1 and v2
                ObjectMapper mapper = new ObjectMapper();
                Map map = mapper.readValue(response, Map.class);
                if (map.containsKey("code") && map.containsKey("msg")) {
                    Object data = map.get("data");
                    return mapper.writeValueAsString(data);
                } else {
                    return response;
                }
            } catch (IOException e) {
                ex = e;
                logger.warn(CONNECT_FAILED_MESSAGE, request.getURI(), e);
            }
        }

        logger.error(CONNECT_FAILED_MESSAGE, request.getURI(), ex);
        throw new ConnectedFailedException(request.getURI().toString(), statusCode, ex);
    }

    private static String getConnectionPost(HttpRequestBase request, String user, String passwd, Logger logger) throws IOException {
        URL url = new URL(request.getURI().toString());
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setInstanceFollowRedirects(false);
        conn.setRequestMethod(request.getMethod());
        String authEncoding = Base64.getEncoder().encodeToString(String.format("%s:%s", user, passwd).getBytes(StandardCharsets.UTF_8));
        conn.setRequestProperty("Authorization", "Basic " + authEncoding);
        InputStream content = ((HttpPost) request).getEntity().getContent();
        String res = IOUtils.toString(content);
        conn.setDoOutput(true);
        conn.setDoInput(true);
        conn.setConnectTimeout(request.getConfig().getConnectTimeout());
        conn.setReadTimeout(request.getConfig().getSocketTimeout());
        PrintWriter out = new PrintWriter(conn.getOutputStream());
        // send request params
        out.print(res);
        // flush
        out.flush();
        // read response
        return parseResponse(conn, logger);
    }

    private static String getConnectionGet(HttpRequestBase request, String user, String passwd, Logger logger) throws IOException {
        URL realUrl = new URL(request.getURI().toString());
        // open connection
        HttpURLConnection connection = (HttpURLConnection) realUrl.openConnection();
        String authEncoding = Base64.getEncoder().encodeToString(String.format("%s:%s", user, passwd).getBytes(StandardCharsets.UTF_8));
        connection.setRequestProperty("Authorization", "Basic " + authEncoding);

        connection.connect();
        connection.setConnectTimeout(request.getConfig().getConnectTimeout());
        connection.setReadTimeout(request.getConfig().getSocketTimeout());
        return parseResponse(connection, logger);
    }

    private static String parseResponse(HttpURLConnection connection, Logger logger) throws IOException {
        if (connection.getResponseCode() != HttpStatus.SC_OK) {
            logger.warn("Failed to get response from Doris  {}, http code is {}",
                    connection.getURL(), connection.getResponseCode());
            throw new IOException("Failed to get response from Doris");
        }
        StringBuffer result = new StringBuffer();
        try (Scanner scanner = new Scanner(connection.getInputStream(), "utf-8")) {
            while (scanner.hasNext()) {
                result.append(scanner.next());
            }
            return result.toString();
        }
    }

    /**
     * parse table identifier to array.
     *
     * @param tableIdentifier table identifier string
     * @param logger          {@link Logger}
     * @return first element is db name, second element is table name
     * @throws IllegalArgumentException table identifier is illegal
     */
    @VisibleForTesting
    static String[] parseIdentifier(String tableIdentifier, Logger logger) throws IllegalArgumentException {
        logger.trace("Parse identifier '{}'.", tableIdentifier);
        if (StringUtils.isEmpty(tableIdentifier)) {
            logger.error(ILLEGAL_ARGUMENT_MESSAGE, "table.identifier", tableIdentifier);
            throw new IllegalArgumentException("table.identifier", tableIdentifier);
        }
        String[] identifier = tableIdentifier.split("\\.");
        if (identifier.length != 2) {
            logger.error(ILLEGAL_ARGUMENT_MESSAGE, "table.identifier", tableIdentifier);
            throw new IllegalArgumentException("table.identifier", tableIdentifier);
        }
        return identifier;
    }

    /**
     * choice a Doris FE node to request.
     *
     * @param feNodes Doris FE node list, separate be comma
     * @param logger  slf4j logger
     * @return the chosen one Doris FE node
     * @throws IllegalArgumentException fe nodes is illegal
     */
    @VisibleForTesting
    public static String randomEndpoint(String feNodes, Logger logger) throws IllegalArgumentException {
        logger.trace("Parse fenodes '{}'.", feNodes);
        if (StringUtils.isEmpty(feNodes)) {
            logger.error(ILLEGAL_ARGUMENT_MESSAGE, "fenodes", feNodes);
            throw new IllegalArgumentException("fenodes", feNodes);
        }
        List<String> nodes = Arrays.asList(feNodes.split(","));
        Collections.shuffle(nodes);
        return nodes.get(0).trim();
    }

    /**
     * choice a Doris FE node to request.
     *
     * @param feNodes Doris FE node list, separate be comma
     * @param logger  slf4j logger
     * @return the array of Doris FE nodes
     * @throws IllegalArgumentException fe nodes is illegal
     */
    @VisibleForTesting
    static List<String> allEndpoints(String feNodes, Logger logger) {
        logger.trace("Parse fenodes '{}'.", feNodes);
        if (StringUtils.isEmpty(feNodes)) {
            logger.error(ILLEGAL_ARGUMENT_MESSAGE, "fenodes", feNodes);
            throw new StarRocksRuntimeException("fenodes is empty");
        }
        List<String> nodes = Arrays.stream(feNodes.split(",")).map(String::trim).collect(Collectors.toList());
        Collections.shuffle(nodes);
        return nodes;
    }

    /**
     * choice a StarRocks BE node to request.
     *
     * @param options configuration of request
     * @param logger  slf4j logger
     * @return the chosen one StarRocks BE node
     * @throws IllegalArgumentException BE nodes is illegal
     */
    @VisibleForTesting
    public static String randomBackend(StarRocksOptions options, StarRocksReadOptions readOptions, Logger logger) throws StarRocksException, IOException {
        List<BackendV2.BackendRowV2> backends = getBackendsV2(options, readOptions, logger);
        logger.trace("Parse beNodes '{}'.", backends);
        if (backends == null || backends.isEmpty()) {
            logger.error(ILLEGAL_ARGUMENT_MESSAGE, "beNodes", backends);
            throw new IllegalArgumentException("beNodes", String.valueOf(backends));
        }
        Collections.shuffle(backends);
        BackendV2.BackendRowV2 backend = backends.get(0);
        return backend.getIp() + ":" + backend.getHttpPort();
    }

    public static String getBackend(StarRocksOptions options, StarRocksReadOptions readOptions, Logger logger) throws StarRocksRuntimeException {
        try {
            return randomBackend(options, readOptions, logger);
        } catch (Exception e) {
            throw new StarRocksRuntimeException("Failed to get backend via " + options.getFenodes(), e);
        }
    }

    /**
     * get StarRocks BE nodes to request.
     *
     * @param options configuration of request
     * @param logger  slf4j logger
     * @return the chosen one StarRocks BE node
     * @throws IllegalArgumentException BE nodes is illegal
     *
     * This method is deprecated. Because it needs ADMIN_PRIV to get backends, which is not suitable for common users.
     * Use getBackendsV2 instead
     */
    @Deprecated
    @VisibleForTesting
    static List<BackendRow> getBackends(StarRocksOptions options, StarRocksReadOptions readOptions, Logger logger) throws StarRocksException, IOException {
        String feNodes = options.getFenodes();
        String feNode = randomEndpoint(feNodes, logger);
        String beUrl = "http://" + feNode + BACKENDS;
        HttpGet httpGet = new HttpGet(beUrl);
        String response = send(options, readOptions, httpGet, logger);
        logger.info("Backend Info:{}", response);
        List<BackendRow> backends = parseBackend(response, logger);
        return backends;
    }

    @Deprecated
    static List<BackendRow> parseBackend(String response, Logger logger) throws StarRocksException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        Backend backend;
        try {
            backend = mapper.readValue(response, Backend.class);
        } catch (JsonParseException e) {
            String errMsg = "StarRocks BE's response is not a json. res: " + response;
            logger.error(errMsg, e);
            throw new StarRocksException(errMsg, e);
        } catch (JsonMappingException e) {
            String errMsg = "StarRocks BE's response cannot map to schema. res: " + response;
            logger.error(errMsg, e);
            throw new StarRocksException(errMsg, e);
        } catch (IOException e) {
            String errMsg = "Parse StarRocks BE's response to json failed. res: " + response;
            logger.error(errMsg, e);
            throw new StarRocksException(errMsg, e);
        }

        if (backend == null) {
            logger.error(SHOULD_NOT_HAPPEN_MESSAGE);
            throw new ShouldNeverHappenException();
        }
        List<BackendRow> backendRows = backend.getRows().stream().filter(v -> v.getAlive()).collect(Collectors.toList());
        logger.debug("Parsing schema result is '{}'.", backendRows);
        return backendRows;
    }

    /**
     * get StarRocks BE nodes to request.
     *
     * @param options configuration of request
     * @param logger  slf4j logger
     * @return the chosen one StarRocks BE node
     * @throws IllegalArgumentException BE nodes is illegal
     */
    public static List<BackendV2.BackendRowV2> getBackendsV2(StarRocksOptions options, StarRocksReadOptions readOptions, Logger logger) {
        String feNodes = options.getFenodes();
        List<String> feNodeList = allEndpoints(feNodes, logger);
        for (String feNode: feNodeList) {
            try {
                String beUrl = "http://" + feNode + BACKENDS_V2;
                HttpGet httpGet = new HttpGet(beUrl);
                String response = send(options, readOptions, httpGet, logger);
                logger.info("Backend Info:{}", response);
                List<BackendV2.BackendRowV2> backends = parseBackendV2(response, logger);
                return backends;
            } catch (ConnectedFailedException e) {
                logger.info("StarRocks FE node {} is unavailable: {}, Request the next StarRocks FE node", feNode, e.getMessage());
            }
        }
        String errMsg = "No StarRocks FE is available, please check configuration";
        logger.error(errMsg);
        throw new StarRocksRuntimeException(errMsg);
    }

    static List<BackendV2.BackendRowV2> parseBackendV2(String response, Logger logger) {
        ObjectMapper mapper = new ObjectMapper();
        BackendV2 backend;
        try {
            backend = mapper.readValue(response, BackendV2.class);
        } catch (JsonParseException e) {
            String errMsg = "StarRocks BE's response is not a json. res: " + response;
            logger.error(errMsg, e);
            throw new StarRocksRuntimeException(errMsg, e);
        } catch (JsonMappingException e) {
            String errMsg = "StarRocks BE's response cannot map to schema. res: " + response;
            logger.error(errMsg, e);
            throw new StarRocksRuntimeException(errMsg, e);
        } catch (IOException e) {
            String errMsg = "Parse StarRocks BE's response to json failed. res: " + response;
            logger.error(errMsg, e);
            throw new StarRocksRuntimeException(errMsg, e);
        }

        if (backend == null) {
            logger.error(SHOULD_NOT_HAPPEN_MESSAGE);
            throw new ShouldNeverHappenException();
        }
        List<BackendV2.BackendRowV2> backendRows = backend.getBackends();
        logger.debug("Parsing schema result is '{}'.", backendRows);
        return backendRows;
    }

    /**
     * get a valid URI to connect StarRocks FE.
     *
     * @param options configuration of request
     * @param logger  {@link Logger}
     * @return uri string
     * @throws IllegalArgumentException throw when configuration is illegal
     */
    @VisibleForTesting
    static String getUriStr(StarRocksOptions options, Logger logger) throws IllegalArgumentException {
        String[] identifier = parseIdentifier(options.getTableIdentifier(), logger);
        return "http://" +
                randomEndpoint(options.getFenodes(), logger) + API_PREFIX +
                "/" + identifier[0] +
                "/" + identifier[1] +
                "/";
    }

    /**
     * discover StarRocks table schema from StarRocks FE.
     *
     * @param options configuration of request
     * @param logger  slf4j logger
     * @return StarRocks table schema
     * @throws StarRocksException throw when discover failed
     */
    public static Schema getSchema(StarRocksOptions options, StarRocksReadOptions readOptions, Logger logger)
            throws StarRocksException {
        logger.trace("Finding schema.");
        HttpGet httpGet = new HttpGet(getUriStr(options, logger) + SCHEMA);
        String response = send(options, readOptions, httpGet, logger);
        logger.debug("Find schema response is '{}'.", response);
        return parseSchema(response, logger);
    }

    public static boolean isUniqueKeyType(StarRocksOptions options, StarRocksReadOptions readOptions, Logger logger)
            throws StarRocksRuntimeException {
        try {
            return UNIQUE_KEYS_TYPE.equals(getSchema(options, readOptions, logger).getKeysType());
        } catch (Exception e) {
            throw new StarRocksRuntimeException(e);
        }
    }
    /**
     * translate StarRocks FE response to inner {@link Schema} struct.
     *
     * @param response StarRocks FE response
     * @param logger   {@link Logger}
     * @return inner {@link Schema} struct
     * @throws StarRocksException throw when translate failed
     */
    @VisibleForTesting
    public static Schema parseSchema(String response, Logger logger) throws StarRocksException {
        logger.trace("Parse response '{}' to schema.", response);
        ObjectMapper mapper = new ObjectMapper();
        Schema schema;
        try {
            schema = mapper.readValue(response, Schema.class);
        } catch (JsonParseException e) {
            String errMsg = "StarRocks FE's response is not a json. res: " + response;
            logger.error(errMsg, e);
            throw new StarRocksException(errMsg, e);
        } catch (JsonMappingException e) {
            String errMsg = "StarRocks FE's response cannot map to schema. res: " + response;
            logger.error(errMsg, e);
            throw new StarRocksException(errMsg, e);
        } catch (IOException e) {
            String errMsg = "Parse StarRocks FE's response to json failed. res: " + response;
            logger.error(errMsg, e);
            throw new StarRocksException(errMsg, e);
        }

        if (schema == null) {
            logger.error(SHOULD_NOT_HAPPEN_MESSAGE);
            throw new ShouldNeverHappenException();
        }

        if (schema.getStatus() != REST_RESPONSE_STATUS_OK) {
            String errMsg = "StarRocks FE's response is not OK, status is " + schema.getStatus();
            logger.error(errMsg);
            throw new StarRocksException(errMsg);
        }
        logger.debug("Parsing schema result is '{}'.", schema);
        return schema;
    }

    /**
     * find StarRocks partitions from StarRocks FE.
     *
     * @param options configuration of request
     * @param logger  {@link Logger}
     * @return a list of StarRocks partitions
     * @throws StarRocksException throw when find partition failed
     */
    public static List<PartitionDefinition> findPartitions(StarRocksOptions options, StarRocksReadOptions readOptions, Logger logger) throws StarRocksException {
        String[] tableIdentifiers = parseIdentifier(options.getTableIdentifier(), logger);
        String readFields = StringUtils.isBlank(readOptions.getReadFields()) ? "*" : readOptions.getReadFields();
        String sql = "select " + readFields +
                " from `" + tableIdentifiers[0] + "`.`" + tableIdentifiers[1] + "`";
        if (!StringUtils.isEmpty(readOptions.getFilterQuery())) {
            sql += " where " + readOptions.getFilterQuery();
        }
        logger.debug("Query SQL Sending to StarRocks FE is: '{}'.", sql);

        HttpPost httpPost = new HttpPost(getUriStr(options, logger) + QUERY_PLAN);
        String entity = "{\"sql\": \"" + sql + "\"}";
        logger.debug("Post body Sending to StarRocks FE is: '{}'.", entity);
        StringEntity stringEntity = new StringEntity(entity, StandardCharsets.UTF_8);
        stringEntity.setContentEncoding("UTF-8");
        stringEntity.setContentType("application/json");
        httpPost.setEntity(stringEntity);

        String resStr = send(options, readOptions, httpPost, logger);
        logger.debug("Find partition response is '{}'.", resStr);
        QueryPlan queryPlan = getQueryPlan(resStr, logger);
        Map<String, List<Long>> be2Tablets = selectBeForTablet(queryPlan, logger);
        return tabletsMapToPartition(
                options,
                readOptions,
                be2Tablets,
                queryPlan.getOpaqued_query_plan(),
                tableIdentifiers[0],
                tableIdentifiers[1],
                logger);
    }

    /**
     * translate StarRocks FE response string to inner {@link QueryPlan} struct.
     *
     * @param response StarRocks FE response string
     * @param logger   {@link Logger}
     * @return inner {@link QueryPlan} struct
     * @throws StarRocksException throw when translate failed.
     */
    @VisibleForTesting
    static QueryPlan getQueryPlan(String response, Logger logger) throws StarRocksException {
        ObjectMapper mapper = new ObjectMapper();
        QueryPlan queryPlan;
        try {
            queryPlan = mapper.readValue(response, QueryPlan.class);
        } catch (JsonParseException e) {
            String errMsg = "StarRocks FE's response is not a json. res: " + response;
            logger.error(errMsg, e);
            throw new StarRocksException(errMsg, e);
        } catch (JsonMappingException e) {
            String errMsg = "StarRocks FE's response cannot map to schema. res: " + response;
            logger.error(errMsg, e);
            throw new StarRocksException(errMsg, e);
        } catch (IOException e) {
            String errMsg = "Parse StarRocks FE's response to json failed. res: " + response;
            logger.error(errMsg, e);
            throw new StarRocksException(errMsg, e);
        }

        if (queryPlan == null) {
            logger.error(SHOULD_NOT_HAPPEN_MESSAGE);
            throw new ShouldNeverHappenException();
        }

        if (queryPlan.getStatus() != REST_RESPONSE_STATUS_OK) {
            String errMsg = "StarRocks FE's response is not OK, status is " + queryPlan.getStatus();
            logger.error(errMsg);
            throw new StarRocksException(errMsg);
        }
        logger.debug("Parsing partition result is '{}'.", queryPlan);
        return queryPlan;
    }

    /**
     * select which StarRocks BE to get tablet data.
     *
     * @param queryPlan {@link QueryPlan} translated from StarRocks FE response
     * @param logger    {@link Logger}
     * @return BE to tablets {@link Map}
     * @throws StarRocksException throw when select failed.
     */
    @VisibleForTesting
    static Map<String, List<Long>> selectBeForTablet(QueryPlan queryPlan, Logger logger) throws StarRocksException {
        Map<String, List<Long>> be2Tablets = new HashMap<>();
        for (Map.Entry<String, Tablet> part : queryPlan.getPartitions().entrySet()) {
            logger.debug("Parse tablet info: '{}'.", part);
            long tabletId;
            try {
                tabletId = Long.parseLong(part.getKey());
            } catch (NumberFormatException e) {
                String errMsg = "Parse tablet id '" + part.getKey() + "' to long failed.";
                logger.error(errMsg, e);
                throw new StarRocksException(errMsg, e);
            }
            String target = null;
            int tabletCount = Integer.MAX_VALUE;
            for (String candidate : part.getValue().getRoutings()) {
                logger.trace("Evaluate StarRocks BE '{}' to tablet '{}'.", candidate, tabletId);
                if (!be2Tablets.containsKey(candidate)) {
                    logger.debug("Choice a new StarRocks BE '{}' for tablet '{}'.", candidate, tabletId);
                    List<Long> tablets = new ArrayList<>();
                    be2Tablets.put(candidate, tablets);
                    target = candidate;
                    break;
                } else {
                    if (be2Tablets.get(candidate).size() < tabletCount) {
                        target = candidate;
                        tabletCount = be2Tablets.get(candidate).size();
                        logger.debug("Current candidate StarRocks BE to tablet '{}' is '{}' with tablet count {}.",
                                tabletId, target, tabletCount);
                    }
                }
            }
            if (target == null) {
                String errMsg = "Cannot choice StarRocks BE for tablet " + tabletId;
                logger.error(errMsg);
                throw new StarRocksException(errMsg);
            }

            logger.debug("Choice StarRocks BE '{}' for tablet '{}'.", target, tabletId);
            be2Tablets.get(target).add(tabletId);
        }
        return be2Tablets;
    }

    /**
     * tablet count limit for one StarRocks RDD partition
     *
     * @param readOptions configuration of request
     * @param logger      {@link Logger}
     * @return tablet count limit
     */
    @VisibleForTesting
    static int tabletCountLimitForOnePartition(StarRocksReadOptions readOptions, Logger logger) {
        int tabletsSize = STARROCKS_TABLET_SIZE_DEFAULT;
        if (readOptions.getRequestTabletSize() != null) {
            tabletsSize = readOptions.getRequestTabletSize();
        }
        if (tabletsSize < STARROCKS_TABLET_SIZE_MIN) {
            logger.warn("{} is less than {}, set to default value {}.",
                    STARROCKS_TABLET_SIZE, STARROCKS_TABLET_SIZE_MIN, STARROCKS_TABLET_SIZE_MIN);
            tabletsSize = STARROCKS_TABLET_SIZE_MIN;
        }
        logger.debug("Tablet size is set to {}.", tabletsSize);
        return tabletsSize;
    }

    /**
     * translate BE tablets map to StarRocks RDD partition.
     *
     * @param options          configuration of request
     * @param be2Tablets       BE to tablets {@link Map}
     * @param opaquedQueryPlan StarRocks BE execute plan getting from StarRocks FE
     * @param database         database name of StarRocks table
     * @param table            table name of StarRocks table
     * @param logger           {@link Logger}
     * @return StarRocks RDD partition {@link List}
     * @throws IllegalArgumentException throw when translate failed
     */
    @VisibleForTesting
    static List<PartitionDefinition> tabletsMapToPartition(StarRocksOptions options, StarRocksReadOptions readOptions, Map<String, List<Long>> be2Tablets,
                                                           String opaquedQueryPlan, String database, String table, Logger logger)
            throws IllegalArgumentException {
        int tabletsSize = tabletCountLimitForOnePartition(readOptions, logger);
        List<PartitionDefinition> partitions = new ArrayList<>();
        for (Map.Entry<String, List<Long>> beInfo : be2Tablets.entrySet()) {
            logger.debug("Generate partition with beInfo: '{}'.", beInfo);
            HashSet<Long> tabletSet = new HashSet<>(beInfo.getValue());
            beInfo.getValue().clear();
            beInfo.getValue().addAll(tabletSet);
            int first = 0;
            while (first < beInfo.getValue().size()) {
                Set<Long> partitionTablets = new HashSet<>(beInfo.getValue().subList(
                        first, Math.min(beInfo.getValue().size(), first + tabletsSize)));
                first = first + tabletsSize;
                PartitionDefinition partitionDefinition =
                        new PartitionDefinition(database, table,
                                beInfo.getKey(), partitionTablets, opaquedQueryPlan);
                logger.debug("Generate one PartitionDefinition '{}'.", partitionDefinition);
                partitions.add(partitionDefinition);
            }
        }
        return partitions;
    }


}

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

package com.nio.starRocks.flink.sink.committer;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.connector.sink.Committer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.nio.starRocks.flink.cfg.StarRocksOptions;
import com.nio.starRocks.flink.cfg.StarRocksReadOptions;
import com.nio.starRocks.flink.exception.StarRocksRuntimeException;
import com.nio.starRocks.flink.rest.RestService;
import com.nio.starRocks.flink.sink.BackendUtil;
import com.nio.starRocks.flink.sink.StarRocksCommittable;
import com.nio.starRocks.flink.sink.HttpPutBuilder;
import com.nio.starRocks.flink.sink.HttpUtil;
import com.nio.starRocks.flink.sink.ResponseUtil;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.nio.starRocks.flink.sink.LoadStatus.FAIL;

/**
 * The committer to commit transaction.
 */
public class StarRocksCommitter implements Committer<StarRocksCommittable> {
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksCommitter.class);
    private static final String commitPattern = "http://%s/api/%s/_stream_load_2pc";
    private final CloseableHttpClient httpClient;
    private final StarRocksOptions dorisOptions;
    private final StarRocksReadOptions starRocksReadOptions;
    private final ObjectMapper jsonMapper = new ObjectMapper();
    private final BackendUtil backendUtil;

    int maxRetry;

    public StarRocksCommitter(StarRocksOptions dorisOptions, StarRocksReadOptions dorisReadOptions, int maxRetry) {
        this(dorisOptions, dorisReadOptions, maxRetry, new HttpUtil().getHttpClient());
    }

    public StarRocksCommitter(StarRocksOptions dorisOptions, StarRocksReadOptions dorisReadOptions, int maxRetry, CloseableHttpClient client) {
        this.dorisOptions = dorisOptions;
        this.starRocksReadOptions = dorisReadOptions;
        this.maxRetry = maxRetry;
        this.httpClient = client;
        this.backendUtil = StringUtils.isNotEmpty(dorisOptions.getBenodes()) ? new BackendUtil(
                dorisOptions.getBenodes())
                : new BackendUtil(RestService.getBackendsV2(dorisOptions, dorisReadOptions, LOG));
    }

    @Override
    public List<StarRocksCommittable> commit(List<StarRocksCommittable> committableList) throws IOException {
        for (StarRocksCommittable committable : committableList) {
            commitTransaction(committable);
        }
        return Collections.emptyList();
    }

    private void commitTransaction(StarRocksCommittable committable) throws IOException {
        //basic params
        HttpPutBuilder builder = new HttpPutBuilder()
                .addCommonHeader()
                .baseAuth(dorisOptions.getUsername(), dorisOptions.getPassword())
                .addTxnId(committable.getTxnID())
                .commit();

        //hostPort
        String hostPort = committable.getHostPort();

        LOG.info("commit txn {} to host {}", committable.getTxnID(), hostPort);
        int retry = 0;
        while (retry++ <= maxRetry) {
            //get latest-url
            String url = String.format(commitPattern, hostPort, committable.getDb());
            HttpPut httpPut = builder.setUrl(url).setEmptyEntity().build();

            // http execute...
            try (CloseableHttpResponse response = httpClient.execute(httpPut)) {
                StatusLine statusLine = response.getStatusLine();
                if (200 == statusLine.getStatusCode()) {
                    if (response.getEntity() != null) {
                        String loadResult = EntityUtils.toString(response.getEntity());
                        Map<String, String> res = jsonMapper.readValue(loadResult, new TypeReference<HashMap<String, String>>() {
                        });
                        if (res.get("status").equals(FAIL) && !ResponseUtil.isCommitted(res.get("msg"))) {
                            throw new StarRocksRuntimeException("Commit failed " + loadResult);
                        } else {
                            LOG.info("load result {}", loadResult);
                        }
                    }
                    return;
                }
                String reasonPhrase = statusLine.getReasonPhrase();
                LOG.warn("commit failed with {}, reason {}", hostPort, reasonPhrase);
                if (retry == maxRetry) {
                    throw new StarRocksRuntimeException("stream load error: " + reasonPhrase);
                }
                hostPort = backendUtil.getAvailableBackend();
            } catch (IOException e) {
                LOG.error("commit transaction failed: ", e);
                if (retry == maxRetry) {
                    throw new IOException("commit transaction failed: {}", e);
                }
                hostPort = backendUtil.getAvailableBackend();
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (httpClient != null) {
            httpClient.close();
        }
    }
}

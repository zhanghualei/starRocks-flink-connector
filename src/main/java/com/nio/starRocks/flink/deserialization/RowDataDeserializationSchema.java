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
package com.nio.starRocks.flink.deserialization;

import com.nio.starRocks.flink.deserialization.converter.StarRocksRowConverter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import java.util.List;


/**
 * A simple implementation of {@link StarRocksDeserializationSchema} which converts the received
 * list record into {@link GenericRowData}.
 */
public class RowDataDeserializationSchema implements StarRocksDeserializationSchema<RowData> {

    private final StarRocksRowConverter rowConverter;

    public RowDataDeserializationSchema(RowType rowType) {
        this.rowConverter = new StarRocksRowConverter(rowType);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return TypeInformation.of(RowData.class);
    }

    @Override
    public void deserialize(List<?> record, Collector<RowData> out) throws Exception {
        RowData row = rowConverter.convertInternal(record);
        out.collect(row);
    }
}

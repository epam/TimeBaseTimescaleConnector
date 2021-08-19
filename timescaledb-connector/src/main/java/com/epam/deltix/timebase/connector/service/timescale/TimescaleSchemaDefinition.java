/*
 * Copyright 2021 EPAM Systems, Inc
 *
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. Licensed under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.epam.deltix.timebase.connector.service.timescale;

import com.epam.deltix.gflog.api.*;
import com.epam.deltix.qsrv.hf.pub.md.ClassDescriptor;
import com.epam.deltix.qsrv.hf.pub.md.RecordClassDescriptor;
import com.epam.deltix.qsrv.hf.tickdb.pub.DXTickStream;
import com.epam.deltix.timebase.connector.model.schema.TimescaleColumn;
import com.epam.deltix.timebase.connector.model.schema.TimescaleSchema;
import com.epam.deltix.timebase.connector.service.timebase.TimebaseDescriptorService;
import com.epam.deltix.timebase.connector.util.RecordClassDescriptorUtils;
import com.epam.deltix.timebase.messages.schema.ClassDescriptorInfo;
import com.epam.deltix.util.collections.generated.ObjectArrayList;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

@Service
@AllArgsConstructor
public class TimescaleSchemaDefinition {

    private static final Log LOG = LogFactory.getLog(TimescaleSchemaDefinition.class);

    private final TimebaseDescriptorService descriptorService;
    //private final JdbcTemplate jdbcTemplate;

    public TimescaleSchema getTimebaseSchemaDefinition(ClassDescriptor[] descriptors, String streamName) {
        TimescaleSchema schema = new TimescaleSchema();
        schema.setName(streamName);

        List<RecordClassDescriptor> baseClassDescriptors = RecordClassDescriptorUtils.getBaseClassDescriptors(descriptors);
        schema.setColumns(descriptorService.getColumns(baseClassDescriptors));
        schema.setPolymorphic(baseClassDescriptors.size() > 1 ? Boolean.TRUE : Boolean.FALSE);

        //add Id, EventTime, symbol, instrumentType
        TimescaleColumn id = TimescaleColumn.builder()
                .dataType(TimescaleColumn.TimescaleDataType.SERIAL)
                .relatedDescriptors(Collections.emptyList())
                .name("Id")
                .build();
        schema.getColumns().add(id);

        TimescaleColumn eventTime = TimescaleColumn.builder()
                .dataType(TimescaleColumn.TimescaleDataType.DATETIME)
                .relatedDescriptors(Collections.emptyList())
                .name("EventTime")
                .build();
        schema.getColumns().add(eventTime);

        TimescaleColumn symbol = TimescaleColumn.builder()
                .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                .relatedDescriptors(Collections.emptyList())
                .name("Symbol")
                .build();
        schema.getColumns().add(symbol);
        //end

        schema.setPrimaryKey(new TimescaleColumn[]{id, eventTime});

        return schema;
    }

    public TimescaleSchema getTimebaseSchemaDefinition(DXTickStream stream) {
        String streamName = stream.getName();
        ClassDescriptor[] allDescriptors = stream.getAllDescriptors();

        return getTimebaseSchemaDefinition(allDescriptors, streamName);
    }

    public TimescaleSchema getTimebaseSchemaDefinition(ObjectArrayList<ClassDescriptorInfo> classDescriptors, String streamName) {
        TimescaleSchema schema = new TimescaleSchema();
        schema.setName(streamName);

        schema.setColumns(descriptorService.getColumnsFromMessages(classDescriptors));

        //add Id, EventTime, symbol, instrumentType
        TimescaleColumn id = TimescaleColumn.builder()
                .dataType(TimescaleColumn.TimescaleDataType.SERIAL)
                .relatedDescriptors(Collections.emptyList())
                .name("Id")
                .build();
        schema.getColumns().add(id);

        TimescaleColumn eventTime = TimescaleColumn.builder()
                .dataType(TimescaleColumn.TimescaleDataType.DATETIME)
                .relatedDescriptors(Collections.emptyList())
                .name("EventTime")
                .build();
        schema.getColumns().add(eventTime);

        TimescaleColumn symbol = TimescaleColumn.builder()
                .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                .relatedDescriptors(Collections.emptyList())
                .name("Symbol")
                .build();
        schema.getColumns().add(symbol);

        schema.setPrimaryKey(new TimescaleColumn[]{id, eventTime});

        return schema;
    }

    //TODO implement restoring TimescaleSchema from Timescale
/*    public TimescaleSchema getTimescaleSchemaDefinition(String tableName) {
        TimescaleSchema schema = new TimescaleSchema();
        schema.setName(tableName);
        Set<TimescaleColumn> columns = new HashSet<>();
        try {
            Connection connection = jdbcTemplate.getDataSource().getConnection();
            ResultSet resultSet = connection.createStatement().executeQuery(String.format("select * from %s limit 1", tableName));
            ResultSetMetaData rsMetaData = resultSet.getMetaData();
            int columnCount = rsMetaData.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = rsMetaData.getColumnName(i);
                int columnType = rsMetaData.getColumnType(i);
                String typeName = JDBCType.valueOf(columnType).getName();
                int precision = rsMetaData.getPrecision(i);
                int scale = rsMetaData.getScale(i);
                String columnClassName = rsMetaData.getColumnClassName(i);

                System.out.println("Column: " + columnName + " type: " + typeName + " precision: " + precision + " scale: " + scale + " columnClassName: " + columnClassName);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return schema;
    }*/
}

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
package com.epam.deltix.timebase.connector.service.timebase;

import com.epam.deltix.qsrv.hf.pub.md.ClassDataType;
import com.epam.deltix.qsrv.hf.pub.md.DataField;
import com.epam.deltix.qsrv.hf.pub.md.DataType;
import com.epam.deltix.qsrv.hf.pub.md.RecordClassDescriptor;
import com.epam.deltix.timebase.connector.model.schema.TimescaleColumn;
import com.epam.deltix.timebase.connector.util.ConnectorUtils;
import com.epam.deltix.timebase.connector.util.MigrationUtils;
import com.epam.deltix.timebase.connector.util.RecordClassDescriptorUtils;
import com.epam.deltix.timebase.messages.schema.*;
import com.epam.deltix.util.collections.generated.ObjectArrayList;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class TimebaseDescriptorService {

    public List<TimescaleColumn> getColumns(List<RecordClassDescriptor> descriptors) {
        List<TimescaleColumn> columns = new ArrayList<>();

        for (RecordClassDescriptor descriptor : descriptors) {
            columns.addAll(getColumns(descriptor, null));
        }

        return filterTimescaleColumns(columns);
    }

    public List<TimescaleColumn> getColumnsFromMessages(List<ClassDescriptorInfo> descriptorMessages) {
        List<TimescaleColumn> columns = new ArrayList<>();

        List<com.epam.deltix.timebase.messages.schema.RecordClassDescriptor> baseDescriptors = RecordClassDescriptorUtils
                .getBaseClassDescriptors(descriptorMessages.toArray(new ClassDescriptor[descriptorMessages.size()]));

        Map<CharSequence, ClassDescriptorInfo> descriptorsMap = descriptorMessages
                .stream()
                .collect(Collectors.toMap(com.epam.deltix.timebase.messages.schema.ClassDescriptorInfo::getName, Function.identity()));
        for (com.epam.deltix.timebase.messages.schema.RecordClassDescriptor descriptor : baseDescriptors) {
            columns.addAll(getColumns(descriptor, null, descriptorsMap));
        }

        return filterTimescaleColumns(columns);
    }

    private List<TimescaleColumn> getColumns(RecordClassDescriptor descriptor, String parentFieldName) {
        List<TimescaleColumn> columns = new ArrayList<>();

        if (!descriptor.isAbstract()) {
            String descriptorColumnName;
            if (parentFieldName != null) {
                descriptorColumnName = parentFieldName + "_" + "descriptor_name";
            } else {
                descriptorColumnName = "descriptor_name";
            }

            columns.add(TimescaleColumn.builder()
                    .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                    .relatedDescriptors(Collections.singletonList(descriptor.getName()))
                    .name(descriptorColumnName)
                    .build());
        }

        RecordClassDescriptor parentDescriptor = descriptor.getParent();
        DataField[] fields = descriptor.getFields();

        for (DataField field : fields) {
            DataType dataType = field.getType();

            if (dataType instanceof ClassDataType) {
                ClassDataType classDataType = (ClassDataType) dataType;
                RecordClassDescriptor[] classDescriptors = classDataType.getDescriptors();
                for (RecordClassDescriptor classDescriptor : classDescriptors) {
                    columns.addAll(getColumns(classDescriptor, parentFieldName == null ? field.getName() : parentFieldName + "_" + field.getName()));
                }
            } else {
                columns.add(ConnectorUtils.convert(field, parentFieldName, descriptor.getName()));
            }
        }

        if (parentDescriptor != null) {
            columns.addAll(getColumns(parentDescriptor, parentFieldName));
        }

        return columns;
    }

    private List<TimescaleColumn> getColumns(com.epam.deltix.timebase.messages.schema.RecordClassDescriptor descriptor, String parentFieldName,
                                             Map<CharSequence, com.epam.deltix.timebase.messages.schema.ClassDescriptorInfo> descriptorsMap) {
        List<TimescaleColumn> columns = new ArrayList<>();

        if (!descriptor.isAbstract()) {
            String descriptorColumnName;
            if (parentFieldName != null) {
                descriptorColumnName = parentFieldName + "_" + "descriptor_name";
            } else {
                descriptorColumnName = "descriptor_name";
            }

            columns.add(TimescaleColumn.builder()
                    .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                    .relatedDescriptors(Collections.singletonList(descriptor.getName().toString()))
                    .name(descriptorColumnName)
                    .build());
        }

        com.epam.deltix.timebase.messages.schema.RecordClassDescriptorInfo parentDescriptor = descriptor.getParent();
        ObjectArrayList<DataFieldInfo> fields = descriptor.getDataFields();

        for (DataFieldInfo field : fields) {
            DataTypeInfo dataType = field.getDataType();

            if (dataType instanceof com.epam.deltix.timebase.messages.schema.ClassDataType) {
                com.epam.deltix.timebase.messages.schema.ClassDataType classDataType = (com.epam.deltix.timebase.messages.schema.ClassDataType) dataType;
                ObjectArrayList<ClassDescriptorRefInfo> classDescriptorsRef = classDataType.getTypeDescriptors();
                for (ClassDescriptorRefInfo descriptorRef : classDescriptorsRef) {
                    columns.addAll(getColumns(
                            (com.epam.deltix.timebase.messages.schema.RecordClassDescriptor) descriptorsMap.get(descriptorRef.getName()),
                            parentFieldName == null ? field.getName().toString() : parentFieldName + "_" + field.getName(),
                            descriptorsMap)
                    );
                }
            } else {
                columns.add(MigrationUtils.convert(field, parentFieldName, descriptor.getName().toString()));
            }
        }

        if (parentDescriptor != null) {
            columns.addAll(getColumns((com.epam.deltix.timebase.messages.schema.RecordClassDescriptor) parentDescriptor, parentFieldName, descriptorsMap));
        }

        return columns;
    }

    private List<TimescaleColumn> filterTimescaleColumns(List<TimescaleColumn> columns) {
        return columns.stream()
                .distinct()
                .collect(Collectors.groupingBy(TimescaleColumn::getName))
                .entrySet()
                .stream()
                .map(entry -> {
                    List<TimescaleColumn> columnList = entry.getValue();
                    TimescaleColumn firstColumn = columnList.get(0);

                    if (columnList.size() == 1) {
                        return firstColumn;
                    } else {
                        List<String> relatedDescriptors = columnList.stream()
                                .map(TimescaleColumn::getRelatedDescriptors)
                                .flatMap(List::stream)
                                .collect(Collectors.toList());

                        return TimescaleColumn.builder()
                                .dataType(firstColumn.getDataType())
                                .relatedDescriptors(relatedDescriptors)
                                .name(entry.getKey())
                                .isArray(firstColumn.isArray())
                                .isNotNull(firstColumn.isNotNull())
                                .isUnique(firstColumn.isUnique())
                                .build();
                    }
                }).collect(Collectors.toList());
    }
}

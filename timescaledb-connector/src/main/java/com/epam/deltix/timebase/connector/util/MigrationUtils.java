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
package com.epam.deltix.timebase.connector.util;

import com.epam.deltix.timebase.connector.model.schema.TimescaleColumn;
import com.epam.deltix.timebase.messages.schema.*;

import java.util.Collections;

public class MigrationUtils {

    public static TimescaleColumn convert(DataFieldInfo dataField, String parentFieldName, String descriptorName) {
        if (dataField == null) {
            throw new IllegalArgumentException("Timebase data field is not specified.");
        }

        DataTypeInfo dataType = dataField.getDataType();
        String name = dataField.getName().toString();
        String fieldName;
        if (parentFieldName != null) {
            fieldName = parentFieldName + "_" + name;
        } else {
            fieldName = name;
        }

        TimescaleColumn.TimescaleDataType timescaleDataType = getTimescaleDataType(dataType);
        boolean isArray = isArray(dataType);

        return TimescaleColumn.builder()
                .name(fieldName)
                .relatedDescriptors(descriptorName == null ? Collections.emptyList() : Collections.singletonList(descriptorName))
                .dataType(timescaleDataType)
                .isArray(isArray)
                .build();
    }

    @Deprecated
    public static String replaceDescriptorName(TimescaleColumn column, String newDescriptorName) {
        // column name pattern: descriptorName_fieldName
        String columnName = column.getName();
        String newShortDescriptorName = ConnectorUtils.getShortDescriptorName(newDescriptorName);

        return String.format("%s_%s", newShortDescriptorName, columnName.replaceAll(".*_", ""));
    }

    private static TimescaleColumn.TimescaleDataType getTimescaleDataType(DataTypeInfo dataType) {
        TimescaleColumn.TimescaleDataType timescaleDataType;
        if (dataType instanceof IntegerDataType) {
            IntegerDataType integerDataType = (IntegerDataType) dataType;
            if ("INT64".equals(integerDataType.getEncoding())) {
                timescaleDataType = TimescaleColumn.TimescaleDataType.LONG;
            } else {
                timescaleDataType = TimescaleColumn.TimescaleDataType.INTEGER;
            }
        } else if (dataType instanceof EnumDataType) {
            timescaleDataType = TimescaleColumn.TimescaleDataType.VARCHAR;
        } else if (dataType instanceof BinaryDataType) {
            timescaleDataType = TimescaleColumn.TimescaleDataType.BYTEA;
        } else if (dataType instanceof BooleanDataType) {
            timescaleDataType = TimescaleColumn.TimescaleDataType.BOOLEAN;
        } else if (dataType instanceof ClassDataType) {
            timescaleDataType = TimescaleColumn.TimescaleDataType.JSON;
        } else if (dataType instanceof CharDataType) {
            timescaleDataType = TimescaleColumn.TimescaleDataType.CHAR;
        } else if (dataType instanceof FloatDataType) {
            FloatDataType floatDataType = (FloatDataType) dataType;
            if ("DECIMAL64".equals(floatDataType.getEncoding())) {
                timescaleDataType = TimescaleColumn.TimescaleDataType.DECIMAL64;
            } else {
                timescaleDataType = TimescaleColumn.TimescaleDataType.DECIMAL;
            }
        } else if (dataType instanceof ArrayDataType) {
            ArrayDataType arrayDataType = (ArrayDataType) dataType;
            DataTypeInfo elementDataType = arrayDataType.getElementType();
            timescaleDataType = getTimescaleDataType(elementDataType);
        } else if (dataType instanceof VarcharDataType) {
            timescaleDataType = TimescaleColumn.TimescaleDataType.VARCHAR;
        } else if (dataType instanceof DateTimeDataType) {
            timescaleDataType = TimescaleColumn.TimescaleDataType.DATETIME;
        } else {
            timescaleDataType = TimescaleColumn.TimescaleDataType.TIME;
        }

        return timescaleDataType;
    }

    private static boolean isArray(DataTypeInfo dataType) {
        return dataType instanceof ArrayDataType;
    }
}

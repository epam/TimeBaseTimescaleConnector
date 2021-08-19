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
package com.epam.deltix.timebase.connector.model.schema;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class TimescaleColumn {

    private String name;
    private TimescaleDataType dataType;
    private List<String> relatedDescriptors;
    private boolean isArray;
    private boolean isUnique;
    private boolean isNotNull;

    public enum TimescaleDataType {
        DECIMAL, DECIMAL64, INTEGER, JSON, JSONB, LONG, CHAR, VARCHAR, DATE, TIME, DATETIME, BYTEA, BOOLEAN, UUID, SERIAL
    }
}

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
package com.epam.deltix.timebase.connector.model;

import com.epam.deltix.timebase.messages.SchemaElement;

public class InnerTestClass {

    private String name;
    private String value;

    public InnerTestClass(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public InnerTestClass() {
    }

    @SchemaElement
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @SchemaElement
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}

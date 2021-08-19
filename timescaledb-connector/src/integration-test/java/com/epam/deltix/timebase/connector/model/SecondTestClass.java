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

import com.epam.deltix.qsrv.hf.pub.md.IntegerDataType;
import com.epam.deltix.timebase.messages.*;

public class SecondTestClass extends BaseTestClass {

    private String secondValue1;
    private long secondLongValue;
    private SomeTestEnum enumValue;

    @SchemaType(
            dataType = SchemaDataType.ENUM
    )
    @SchemaElement
    public SomeTestEnum getEnumValue() {
        return enumValue;
    }

    public void setEnumValue(SomeTestEnum enumValue) {
        this.enumValue = enumValue;
    }

    @SchemaElement
    public String getSecondValue1() {
        return secondValue1;
    }

    public void setSecondValue1(String secondValue1) {
        this.secondValue1 = secondValue1;
    }

    @SchemaType(
            encoding = IntegerDataType.ENCODING_INT64,
            dataType = SchemaDataType.INTEGER
    )
    @SchemaElement
    public long getSecondLongValue() {
        return secondLongValue;
    }

    public void setSecondLongValue(long secondLongValue) {
        this.secondLongValue = secondLongValue;
    }

    @Override
    public String toString() {
        return "SecondTestClass{" +
                "secondValue1='" + secondValue1 + '\'' +
                ", secondLongValue=" + secondLongValue +
                ", enumValue=" + enumValue +
                ", stringValue=" + getStringValue() +
                ", decimal64Value=" + getDecimal64Value() +
                ", byteaValue=" + getByteaValue() +
                ", timestamp=" + timestamp +
                ", nanoTime=" + nanoTime +
                ", symbol=" + symbol +
                '}';
    }
}

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

public class FirstTestClass extends BaseTestClass {

    private char charValue;
    private int intValue;
    private long longValue;
    private InnerTestClass innerState;

    @SchemaElement
    public InnerTestClass getInnerState() {
        return innerState;
    }

    public void setInnerState(InnerTestClass innerState) {
        this.innerState = innerState;
    }

    @SchemaType(
            dataType = SchemaDataType.CHAR
    )
    @SchemaElement
    public char getCharValue() {
        return charValue;
    }

    public void setCharValue(char charValue) {
        this.charValue = charValue;
    }

    @SchemaType(
            encoding = IntegerDataType.ENCODING_INT32,
            dataType = SchemaDataType.INTEGER
    )
    @SchemaElement
    public int getIntValue() {
        return intValue;
    }

    public void setIntValue(int intValue) {
        this.intValue = intValue;
    }

    @SchemaType(
            encoding = IntegerDataType.ENCODING_INT64,
            dataType = SchemaDataType.INTEGER
    )
    @SchemaElement
    public long getLongValue() {
        return longValue;
    }

    public void setLongValue(long longValue) {
        this.longValue = longValue;
    }

    @Override
    public String toString() {
        return "FirstTestClass{" +
                "charValue=" + charValue +
                ", intValue=" + intValue +
                ", longValue=" + longValue +
                ", stringValue=" + getStringValue() +
                ", decimal64Value=" + getDecimal64Value() +
                ", byteaValue=" + getByteaValue() +
                ", timestamp=" + timestamp +
                ", nanoTime=" + nanoTime +
                ", symbol=" + symbol +
                '}';
    }
}

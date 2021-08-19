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

import com.epam.deltix.dfp.Decimal64;
import com.epam.deltix.timebase.messages.*;
import com.epam.deltix.util.collections.generated.ByteArrayList;

public abstract class BaseTestClass extends InstrumentMessage {

    private CharSequence stringValue;
    private Decimal64 decimal64Value;
    private ByteArrayList byteaValue;

    @SchemaElement
    public CharSequence getStringValue() {
        return stringValue;
    }

    public void setStringValue(CharSequence stringValue) {
        this.stringValue = stringValue;
    }

    @SchemaType(
            dataType = SchemaDataType.FLOAT,
            encoding = "DECIMAL64"
    )
    @SchemaElement
    public Decimal64 getDecimal64Value() {
        return decimal64Value;
    }

    public void setDecimal64Value(Decimal64 decimal64Value) {
        this.decimal64Value = decimal64Value;
    }

    @SchemaElement
    public ByteArrayList getByteaValue() {
        return byteaValue;
    }

    public void setByteaValue(ByteArrayList byteaValue) {
        this.byteaValue = byteaValue;
    }
}

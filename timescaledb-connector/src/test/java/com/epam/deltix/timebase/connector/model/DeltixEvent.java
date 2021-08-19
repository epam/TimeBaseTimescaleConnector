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

public class DeltixEvent extends BaseEvent {

    private Decimal64 quantity;
    private OwnerEntity owner;
    private String reporter;

    @SchemaType(
            encoding = "DECIMAL64",
            dataType = SchemaDataType.FLOAT
    )
    @SchemaElement
    public Decimal64 getQuantity() {
        return quantity;
    }

    public void setQuantity(Decimal64 quantity) {
        this.quantity = quantity;
    }

    @SchemaElement
    public OwnerEntity getOwner() {
        return owner;
    }

    public void setOwner(OwnerEntity owner) {
        this.owner = owner;
    }

    @SchemaElement
    public String getReporter() {
        return reporter;
    }

    public void setReporter(String reporter) {
        this.reporter = reporter;
    }
}

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
import org.junit.Test;

import static com.epam.deltix.timebase.connector.util.MigrationUtils.replaceDescriptorName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class MigrationUtilsTest {

    @Deprecated
    @Test
    public void testReplaceDescriptorName() {
        TimescaleColumn column = TimescaleColumn.builder()
                .name("FirstClass_FieldName")
                .build();

        assertThat(replaceDescriptorName(column, "deltix.messages.SecondClass"), is("SecondClass_FieldName"));
    }
}

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

import com.epam.deltix.qsrv.hf.pub.md.*;
import com.epam.deltix.timebase.messages.schema.ClassDescriptorRef;
import com.epam.deltix.timebase.messages.schema.ClassDescriptorRefInfo;
import com.epam.deltix.timebase.messages.schema.DataFieldInfo;
import com.epam.deltix.util.collections.generated.ObjectArrayList;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class RecordClassDescriptorUtilsTest {

    @Test
    public void testGetBaseRecordClassDescriptors() {
        RecordClassDescriptor descriptor1 = new RecordClassDescriptor(
                "guid1",
                "descriptor1",
                "title",
                false,
                null
        );

        RecordClassDescriptor descriptor2 = new RecordClassDescriptor(
                "guid1",
                "descriptor2",
                "title",
                false,
                null
        );

        EnumClassDescriptor enumClassDescriptor = new EnumClassDescriptor(
                "enumName",
                "title",
                "value1",
                "value2"
        );

        RecordClassDescriptor descriptor3 = new RecordClassDescriptor(
                "guid1",
                "descriptor3",
                "title",
                true,
                null
        );

        DataField enumDataField = new NonStaticDataField(
                "enumField",
                "title",
                new EnumDataType(false, enumClassDescriptor)
        );

        DataField arrayDataField = new NonStaticDataField(
                "arrayField",
                "title",
                new ArrayDataType(false, new ClassDataType(true, descriptor2))
        );

        DataField classDataField = new NonStaticDataField(
                "classField",
                "title",
                new ClassDataType(false, descriptor1)
        );

        RecordClassDescriptor descriptor4 = new RecordClassDescriptor(
                "guid1",
                "descriptor4",
                "title",
                false,
                descriptor3,
                enumDataField,
                arrayDataField,
                classDataField
        );

        ClassDescriptor[] descriptors = new ClassDescriptor[] {descriptor1, descriptor2, descriptor3, descriptor4, enumClassDescriptor};

        List<RecordClassDescriptor> baseClassDescriptors = RecordClassDescriptorUtils.getBaseClassDescriptors(descriptors);

        assertThat(baseClassDescriptors.size(), is(1));
        assertThat(baseClassDescriptors.get(0), is(descriptor4));
    }

    @Test
    public void testGetBaseRecordClassDescriptorMessages() {
        com.epam.deltix.timebase.messages.schema.RecordClassDescriptor descriptor1 = new com.epam.deltix.timebase.messages.schema.RecordClassDescriptor();
        descriptor1.setIsAbstract(false);
        descriptor1.setParent(null);
        descriptor1.setTitle("title");
        descriptor1.setName("descriptor1");

        com.epam.deltix.timebase.messages.schema.RecordClassDescriptor descriptor2 = new com.epam.deltix.timebase.messages.schema.RecordClassDescriptor();
        descriptor2.setIsAbstract(false);
        descriptor2.setParent(null);
        descriptor2.setTitle("title");
        descriptor2.setName("descriptor2");

        com.epam.deltix.timebase.messages.schema.EnumClassDescriptor enumClassDescriptor = new com.epam.deltix.timebase.messages.schema.EnumClassDescriptor();
        enumClassDescriptor.setValues(new ObjectArrayList<>());
        enumClassDescriptor.setTitle("title");
        enumClassDescriptor.setName("enumName");

        com.epam.deltix.timebase.messages.schema.RecordClassDescriptor descriptor3 = new com.epam.deltix.timebase.messages.schema.RecordClassDescriptor();
        descriptor3.setIsAbstract(true);
        descriptor3.setParent(null);
        descriptor3.setTitle("title");
        descriptor3.setName("descriptor3");

        com.epam.deltix.timebase.messages.schema.DataField enumDataField = new com.epam.deltix.timebase.messages.schema.NonStaticDataField();
        enumDataField.setTitle("title");
        enumDataField.setDataType(new com.epam.deltix.timebase.messages.schema.EnumDataType());
        enumDataField.setName("enumField");

        com.epam.deltix.timebase.messages.schema.DataField arrayDataField = new com.epam.deltix.timebase.messages.schema.NonStaticDataField();
        arrayDataField.setName("arrayField");
        arrayDataField.setTitle("title");
        com.epam.deltix.timebase.messages.schema.ArrayDataType arrayDataType = new com.epam.deltix.timebase.messages.schema.ArrayDataType();
        com.epam.deltix.timebase.messages.schema.ClassDataType classDataType = new com.epam.deltix.timebase.messages.schema.ClassDataType();
        ObjectArrayList<ClassDescriptorRefInfo> classDescriptorsRefs = new ObjectArrayList<>();
        ClassDescriptorRef classDescriptorsRef = new ClassDescriptorRef();
        classDescriptorsRef.setName("descriptor2");
        classDescriptorsRefs.add(classDescriptorsRef);
        classDataType.setTypeDescriptors(classDescriptorsRefs);
        arrayDataType.setElementType(classDataType);
        arrayDataField.setDataType(arrayDataType);

        com.epam.deltix.timebase.messages.schema.DataField classDataField = new com.epam.deltix.timebase.messages.schema.NonStaticDataField();
        com.epam.deltix.timebase.messages.schema.ClassDataType classDataType1 = new com.epam.deltix.timebase.messages.schema.ClassDataType();
        ObjectArrayList<ClassDescriptorRefInfo> classDescriptorsRefs1 = new ObjectArrayList<>();
        ClassDescriptorRef classDescriptorsRef1 = new ClassDescriptorRef();
        classDescriptorsRef1.setName("descriptor1");
        classDescriptorsRefs1.add(classDescriptorsRef1);
        classDataType1.setTypeDescriptors(classDescriptorsRefs1);
        classDataField.setName("classField");
        classDataField.setTitle("title");
        classDataField.setDataType(classDataType1);

        com.epam.deltix.timebase.messages.schema.RecordClassDescriptor descriptor4 = new com.epam.deltix.timebase.messages.schema.RecordClassDescriptor();
        ObjectArrayList<DataFieldInfo> dataFields = new ObjectArrayList<>();
        dataFields.addAll(Arrays.asList(enumDataField, arrayDataField, classDataField));

        descriptor4.setName("descriptor4");
        descriptor4.setTitle("title");
        descriptor4.setIsAbstract(false);
        descriptor4.setParent(descriptor3);
        descriptor4.setDataFields(dataFields);

        com.epam.deltix.timebase.messages.schema.ClassDescriptorInfo[] descriptors = new com.epam.deltix.timebase.messages.schema.ClassDescriptor[] {descriptor1, descriptor2, descriptor3, descriptor4, enumClassDescriptor};

        List<com.epam.deltix.timebase.messages.schema.RecordClassDescriptor> baseClassDescriptors = RecordClassDescriptorUtils.getBaseClassDescriptors(descriptors);

        assertThat(baseClassDescriptors.size(), is(1));
        assertThat(baseClassDescriptors.get(0), is(descriptor4));
    }
}

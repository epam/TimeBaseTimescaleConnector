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

import com.epam.deltix.qsrv.hf.pub.md.ArrayDataType;
import com.epam.deltix.qsrv.hf.pub.md.ClassDataType;
import com.epam.deltix.qsrv.hf.pub.md.ClassDescriptor;
import com.epam.deltix.qsrv.hf.pub.md.DataField;
import com.epam.deltix.qsrv.hf.pub.md.DataType;
import com.epam.deltix.qsrv.hf.pub.md.EnumClassDescriptor;
import com.epam.deltix.qsrv.hf.pub.md.RecordClassDescriptor;
import com.epam.deltix.timebase.messages.schema.*;
import com.epam.deltix.util.collections.generated.ObjectArrayList;
import com.epam.deltix.util.collections.generated.ObjectList;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RecordClassDescriptorUtils {

    public static List<RecordClassDescriptor> getBaseClassDescriptors(ClassDescriptor[] descriptors) {
        if (descriptors.length == 0) {
            throw new IllegalArgumentException("Could not find base RecordClassDescriptor for empty input descriptors.");
        }

        if (descriptors.length == 1) {
            if (descriptors[0] instanceof EnumClassDescriptor) {
                throw new IllegalArgumentException("Could not find base RecordClassDescriptor. Only EnumClassDescriptor found.");
            }

            return Collections.singletonList((RecordClassDescriptor) descriptors[0]);
        }

        Set<String> usedClasses = new HashSet<>();

        List<RecordClassDescriptor> descriptorsList = Arrays.asList(descriptors).stream()
                .filter(descriptor -> descriptor instanceof RecordClassDescriptor)
                .map(descriptor -> (RecordClassDescriptor) descriptor)
                .collect(Collectors.toList());
        descriptorsList.forEach(descriptor -> fillUsedClasses(descriptor, usedClasses));

        return descriptorsList.stream()
                .filter(descriptor -> !usedClasses.contains(descriptor.getName()))
                .collect(Collectors.toList());
    }

    public static List<com.epam.deltix.timebase.messages.schema.RecordClassDescriptor> getBaseClassDescriptors(com.epam.deltix.timebase.messages.schema.ClassDescriptorInfo[] descriptors) {
        if (descriptors.length == 0) {
            throw new IllegalArgumentException("Could not find base RecordClassDescriptor for empty input descriptors.");
        }

        if (descriptors.length == 1) {
            if (descriptors[0] instanceof com.epam.deltix.timebase.messages.schema.EnumClassDescriptor) {
                throw new IllegalArgumentException("Could not find base RecordClassDescriptor. Only EnumClassDescriptor found.");
            }

            return Collections.singletonList((com.epam.deltix.timebase.messages.schema.RecordClassDescriptor) descriptors[0]);
        }

        Set<String> usedClasses = new HashSet<>();

        List<com.epam.deltix.timebase.messages.schema.RecordClassDescriptor> descriptorsList = Arrays.asList(descriptors).stream()
                .filter(descriptor -> descriptor instanceof com.epam.deltix.timebase.messages.schema.RecordClassDescriptor)
                .map(descriptor -> (com.epam.deltix.timebase.messages.schema.RecordClassDescriptor) descriptor)
                .collect(Collectors.toList());
        Map<CharSequence, com.epam.deltix.timebase.messages.schema.RecordClassDescriptor> descriptorsMap = descriptorsList.stream()
                .collect(Collectors.toMap(com.epam.deltix.timebase.messages.schema.RecordClassDescriptor::getName, Function.identity()));
        descriptorsList.forEach(descriptor -> fillUsedClasses(descriptor, usedClasses, descriptorsMap));

        return descriptorsList.stream()
                .filter(descriptor -> !usedClasses.contains(descriptor.getName()))
                .collect(Collectors.toList());
    }

    private static void fillUsedClasses(RecordClassDescriptor descriptor, Set<String> usedClasses) {
        RecordClassDescriptor parent = descriptor.getParent();

        if (usedClasses.contains(descriptor.getName())) {
            return;
        }

        if (parent != null) {
            fillUsedClasses(parent, usedClasses);
            usedClasses.add(parent.getName());
        }

        DataField[] fields = descriptor.getFields();
        for (DataField field : fields) {
            var dataType = field.getType();

            if (dataType instanceof ClassDataType) {
                ClassDataType type = (ClassDataType) dataType;
                RecordClassDescriptor[] classDescriptors = type.getDescriptors();

                for (RecordClassDescriptor d : classDescriptors) {
                    fillUsedClasses(d, usedClasses);
                    usedClasses.add(d.getName());
                }
            } else if (dataType instanceof ArrayDataType) {
                ArrayDataType type = (ArrayDataType) dataType;
                DataType elementDataType = type.getElementDataType();

                if (elementDataType instanceof  ClassDataType) {
                    ClassDataType classElementType = (ClassDataType) elementDataType;
                    RecordClassDescriptor[] descriptors = classElementType.getDescriptors();
                    for (RecordClassDescriptor d : descriptors) {
                        fillUsedClasses(d, usedClasses);
                        usedClasses.add(d.getName());
                    }
                }
            }
        }
    }

    private static void fillUsedClasses(com.epam.deltix.timebase.messages.schema.RecordClassDescriptorInfo descriptor,
                                        Set<String> usedClasses,
                                        Map<CharSequence, com.epam.deltix.timebase.messages.schema.RecordClassDescriptor> descriptorsMap) {
        com.epam.deltix.timebase.messages.schema.RecordClassDescriptorInfo parent = descriptor.getParent();

        if (usedClasses.contains(descriptor.getName())) {
            return;
        }

        if (parent != null) {
            fillUsedClasses(parent, usedClasses, descriptorsMap);
            usedClasses.add(parent.getName().toString());
        }

        ObjectList<DataFieldInfo> dataFields = descriptor.getDataFields();

        if (dataFields == null) {
            return;
        }

        for (int i = 0; i < dataFields.size(); i++) {
            DataFieldInfo field = dataFields.get(i);
            DataTypeInfo dataType = field.getDataType();

            if (dataType instanceof com.epam.deltix.timebase.messages.schema.ClassDataType) {
                com.epam.deltix.timebase.messages.schema.ClassDataType type = (com.epam.deltix.timebase.messages.schema.ClassDataType) dataType;
                ObjectArrayList<ClassDescriptorRefInfo> classDescriptors = type.getTypeDescriptors();

                classDescriptors.forEach(d -> {
                    fillUsedClasses(descriptorsMap.get(d.getName()), usedClasses, descriptorsMap);
                    usedClasses.add(d.getName().toString());
                });
            } else if (dataType instanceof com.epam.deltix.timebase.messages.schema.ArrayDataType) {
                com.epam.deltix.timebase.messages.schema.ArrayDataType type = (com.epam.deltix.timebase.messages.schema.ArrayDataType) dataType;
                DataTypeInfo elementDataType = type.getElementType();

                if (elementDataType instanceof com.epam.deltix.timebase.messages.schema.ClassDataType) {
                    com.epam.deltix.timebase.messages.schema.ClassDataType classElementType =
                            (com.epam.deltix.timebase.messages.schema.ClassDataType) elementDataType;
                    ObjectArrayList<ClassDescriptorRefInfo> descriptors = classElementType.getTypeDescriptors();
                    descriptors.forEach(d -> {
                        fillUsedClasses(descriptorsMap.get(d.getName()), usedClasses, descriptorsMap);
                        usedClasses.add(d.getName().toString());
                    });
                }
            }
        }
    }
}

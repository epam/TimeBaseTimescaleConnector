package deltix.timebase.connector.util;

import deltix.qsrv.hf.pub.md.*;
import deltix.timebase.messages.schema.ClassDescriptorRefInfo;
import deltix.timebase.messages.schema.DataFieldInfo;
import deltix.timebase.messages.schema.DataTypeInfo;
import deltix.util.collections.generated.ObjectArrayList;
import deltix.util.collections.generated.ObjectList;

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

    public static List<deltix.timebase.messages.schema.RecordClassDescriptor> getBaseClassDescriptors(deltix.timebase.messages.schema.ClassDescriptorInfo[] descriptors) {
        if (descriptors.length == 0) {
            throw new IllegalArgumentException("Could not find base RecordClassDescriptor for empty input descriptors.");
        }

        if (descriptors.length == 1) {
            if (descriptors[0] instanceof deltix.timebase.messages.schema.EnumClassDescriptor) {
                throw new IllegalArgumentException("Could not find base RecordClassDescriptor. Only EnumClassDescriptor found.");
            }

            return Collections.singletonList((deltix.timebase.messages.schema.RecordClassDescriptor) descriptors[0]);
        }

        Set<String> usedClasses = new HashSet<>();

        List<deltix.timebase.messages.schema.RecordClassDescriptor> descriptorsList = Arrays.asList(descriptors).stream()
                .filter(descriptor -> descriptor instanceof deltix.timebase.messages.schema.RecordClassDescriptor)
                .map(descriptor -> (deltix.timebase.messages.schema.RecordClassDescriptor) descriptor)
                .collect(Collectors.toList());
        Map<CharSequence, deltix.timebase.messages.schema.RecordClassDescriptor> descriptorsMap = descriptorsList.stream()
                .collect(Collectors.toMap(deltix.timebase.messages.schema.RecordClassDescriptor::getName, Function.identity()));
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
            DataType dataType = field.getType();

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

    private static void fillUsedClasses(deltix.timebase.messages.schema.RecordClassDescriptorInfo descriptor,
                                        Set<String> usedClasses,
                                        Map<CharSequence, deltix.timebase.messages.schema.RecordClassDescriptor> descriptorsMap) {
        deltix.timebase.messages.schema.RecordClassDescriptorInfo parent = descriptor.getParent();

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

            if (dataType instanceof deltix.timebase.messages.schema.ClassDataType) {
                deltix.timebase.messages.schema.ClassDataType type = (deltix.timebase.messages.schema.ClassDataType) dataType;
                ObjectArrayList<ClassDescriptorRefInfo> classDescriptors = type.getTypeDescriptors();

                classDescriptors.forEach(d -> {
                    fillUsedClasses(descriptorsMap.get(d.getName()), usedClasses, descriptorsMap);
                    usedClasses.add(d.getName().toString());
                });
            } else if (dataType instanceof deltix.timebase.messages.schema.ArrayDataType) {
                deltix.timebase.messages.schema.ArrayDataType type = (deltix.timebase.messages.schema.ArrayDataType) dataType;
                DataTypeInfo elementDataType = type.getElementType();

                if (elementDataType instanceof  deltix.timebase.messages.schema.ClassDataType) {
                    deltix.timebase.messages.schema.ClassDataType classElementType =
                            (deltix.timebase.messages.schema.ClassDataType) elementDataType;
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

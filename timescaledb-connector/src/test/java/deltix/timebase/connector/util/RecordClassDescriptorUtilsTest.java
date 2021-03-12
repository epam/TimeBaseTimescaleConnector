package deltix.timebase.connector.util;

import deltix.qsrv.hf.pub.md.*;
import deltix.timebase.messages.schema.ClassDescriptorRef;
import deltix.timebase.messages.schema.ClassDescriptorRefInfo;
import deltix.timebase.messages.schema.DataFieldInfo;
import deltix.util.collections.generated.ObjectArrayList;
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
        deltix.timebase.messages.schema.RecordClassDescriptor descriptor1 = new deltix.timebase.messages.schema.RecordClassDescriptor();
        descriptor1.setIsAbstract(false);
        descriptor1.setParent(null);
        descriptor1.setTitle("title");
        descriptor1.setName("descriptor1");

        deltix.timebase.messages.schema.RecordClassDescriptor descriptor2 = new deltix.timebase.messages.schema.RecordClassDescriptor();
        descriptor2.setIsAbstract(false);
        descriptor2.setParent(null);
        descriptor2.setTitle("title");
        descriptor2.setName("descriptor2");

        deltix.timebase.messages.schema.EnumClassDescriptor enumClassDescriptor = new deltix.timebase.messages.schema.EnumClassDescriptor();
        enumClassDescriptor.setValues(new ObjectArrayList<>());
        enumClassDescriptor.setTitle("title");
        enumClassDescriptor.setName("enumName");

        deltix.timebase.messages.schema.RecordClassDescriptor descriptor3 = new deltix.timebase.messages.schema.RecordClassDescriptor();
        descriptor3.setIsAbstract(true);
        descriptor3.setParent(null);
        descriptor3.setTitle("title");
        descriptor3.setName("descriptor3");

        deltix.timebase.messages.schema.DataField enumDataField = new deltix.timebase.messages.schema.NonStaticDataField();
        enumDataField.setTitle("title");
        enumDataField.setDataType(new deltix.timebase.messages.schema.EnumDataType());
        enumDataField.setName("enumField");

        deltix.timebase.messages.schema.DataField arrayDataField = new deltix.timebase.messages.schema.NonStaticDataField();
        arrayDataField.setName("arrayField");
        arrayDataField.setTitle("title");
        deltix.timebase.messages.schema.ArrayDataType arrayDataType = new deltix.timebase.messages.schema.ArrayDataType();
        deltix.timebase.messages.schema.ClassDataType classDataType = new deltix.timebase.messages.schema.ClassDataType();
        ObjectArrayList<ClassDescriptorRefInfo> classDescriptorsRefs = new ObjectArrayList<>();
        ClassDescriptorRef classDescriptorsRef = new ClassDescriptorRef();
        classDescriptorsRef.setName("descriptor2");
        classDescriptorsRefs.add(classDescriptorsRef);
        classDataType.setTypeDescriptors(classDescriptorsRefs);
        arrayDataType.setElementType(classDataType);
        arrayDataField.setDataType(arrayDataType);

        deltix.timebase.messages.schema.DataField classDataField = new deltix.timebase.messages.schema.NonStaticDataField();
        deltix.timebase.messages.schema.ClassDataType classDataType1 = new deltix.timebase.messages.schema.ClassDataType();
        ObjectArrayList<ClassDescriptorRefInfo> classDescriptorsRefs1 = new ObjectArrayList<>();
        ClassDescriptorRef classDescriptorsRef1 = new ClassDescriptorRef();
        classDescriptorsRef1.setName("descriptor1");
        classDescriptorsRefs1.add(classDescriptorsRef1);
        classDataType1.setTypeDescriptors(classDescriptorsRefs1);
        classDataField.setName("classField");
        classDataField.setTitle("title");
        classDataField.setDataType(classDataType1);

        deltix.timebase.messages.schema.RecordClassDescriptor descriptor4 = new deltix.timebase.messages.schema.RecordClassDescriptor();
        ObjectArrayList<DataFieldInfo> dataFields = new ObjectArrayList<>();
        dataFields.addAll(Arrays.asList(enumDataField, arrayDataField, classDataField));

        descriptor4.setName("descriptor4");
        descriptor4.setTitle("title");
        descriptor4.setIsAbstract(false);
        descriptor4.setParent(descriptor3);
        descriptor4.setDataFields(dataFields);

        deltix.timebase.messages.schema.ClassDescriptorInfo[] descriptors = new deltix.timebase.messages.schema.ClassDescriptor[] {descriptor1, descriptor2, descriptor3, descriptor4, enumClassDescriptor};

        List<deltix.timebase.messages.schema.RecordClassDescriptor> baseClassDescriptors = RecordClassDescriptorUtils.getBaseClassDescriptors(descriptors);

        assertThat(baseClassDescriptors.size(), is(1));
        assertThat(baseClassDescriptors.get(0), is(descriptor4));
    }
}

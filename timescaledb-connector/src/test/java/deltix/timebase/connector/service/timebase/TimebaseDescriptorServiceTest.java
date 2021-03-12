package deltix.timebase.connector.service.timebase;

import deltix.qsrv.hf.pub.md.*;
import deltix.timebase.connector.model.DeltixEvent;
import deltix.timebase.connector.model.TimebaseEvent;
import deltix.timebase.connector.model.schema.TimescaleColumn;
import deltix.timebase.connector.util.RecordClassDescriptorUtils;
import deltix.timebase.messages.schema.ClassDescriptorRef;
import deltix.timebase.messages.schema.ClassDescriptorRefInfo;
import deltix.timebase.messages.schema.DataFieldInfo;
import deltix.util.collections.generated.ObjectArrayList;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

public class TimebaseDescriptorServiceTest {

    private TimebaseDescriptorService descriptorService = new TimebaseDescriptorService();

    @Test
    public void testGetColumns() {
        RecordClassDescriptor descriptor1 = new RecordClassDescriptor(
                "com.deltix.Descriptor1",
                "title",
                false,
                null,
                new NonStaticDataField("name", "title", VarcharDataType.getDefaultInstance()),
                new StaticDataField("value", "title", VarcharDataType.getDefaultInstance(), "default_value")
        );

        RecordClassDescriptor descriptor2 = new RecordClassDescriptor(
                "com.deltix.Descriptor2",
                "title",
                false,
                null,
                new NonStaticDataField("price", "title", FloatDataType.getDefaultInstance()),
                new StaticDataField("description", "title", VarcharDataType.getDefaultInstance(), "default_value"),
                new NonStaticDataField("attribute", "title", new ClassDataType(true, descriptor1))
        );

        RecordClassDescriptor descriptor3 = new RecordClassDescriptor(
                "com.deltix.Descriptor3",
                "title",
                false,
                null,
                new NonStaticDataField("type", "title", VarcharDataType.getDefaultInstance()),
                new NonStaticDataField("code", "title", IntegerDataType.getDefaultInstance()),
                new NonStaticDataField("event", "title", new ClassDataType(true, descriptor2))
        );

        List<TimescaleColumn> actualColumns = descriptorService.getColumns(Collections.singletonList(descriptor3));

        List<TimescaleColumn> expectedColumns = getExpectedColumns();

        assertThat(actualColumns.size(), is(expectedColumns.size()));
        assertThat(actualColumns, containsInAnyOrder(expectedColumns.toArray(new TimescaleColumn[expectedColumns.size()])));
    }

    @Test
    public void testGetColumnsFromIntrospection() {
        Introspector introspector = Introspector.createEmptyMessageIntrospector();

        RecordClassDescriptor[] recordClassDescriptors = new RecordClassDescriptor[2];
        try {
            RecordClassDescriptor firstRecordClassDescriptor = introspector.introspectRecordClass(DeltixEvent.class);
            RecordClassDescriptor secondRecordClassDescriptor = introspector.introspectRecordClass(TimebaseEvent.class);
            recordClassDescriptors[0] = firstRecordClassDescriptor;
            recordClassDescriptors[1] = secondRecordClassDescriptor;
        } catch (Introspector.IntrospectionException ex) {
            ex.printStackTrace();
        }

        List<RecordClassDescriptor> baseClassDescriptors = RecordClassDescriptorUtils.getBaseClassDescriptors(recordClassDescriptors);

        List<TimescaleColumn> actualColumns = descriptorService.getColumns(baseClassDescriptors);

        TimescaleColumn descriptorName = TimescaleColumn.builder()
                .name("descriptor_name")
                .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                .relatedDescriptors(Arrays.asList("deltix.timebase.connector.model.DeltixEvent", "deltix.timebase.connector.model.TimebaseEvent"))
                .build();
        TimescaleColumn ownerDescriptorName = TimescaleColumn.builder()
                .name("owner_descriptor_name")
                .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                .relatedDescriptors(Collections.singletonList("deltix.timebase.connector.model.OwnerEntity"))
                .build();
        TimescaleColumn ownerBusinessGroupDescriptorName = TimescaleColumn.builder()
                .name("owner_businessGroup_descriptor_name")
                .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                .relatedDescriptors(Collections.singletonList("deltix.timebase.connector.model.BusinessGroup"))
                .build();
        TimescaleColumn ownerBusinessGroupGroupId = TimescaleColumn.builder()
                .name("owner_businessGroup_groupId")
                .dataType(TimescaleColumn.TimescaleDataType.LONG)
                .relatedDescriptors(Collections.singletonList("deltix.timebase.connector.model.BusinessGroup"))
                .build();
        TimescaleColumn ownerBusinessGroupGroupName = TimescaleColumn.builder()
                .name("owner_businessGroup_groupName")
                .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                .relatedDescriptors(Collections.singletonList("deltix.timebase.connector.model.BusinessGroup"))
                .build();
        TimescaleColumn ownerEmail = TimescaleColumn.builder()
                .name("owner_email")
                .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                .relatedDescriptors(Collections.singletonList("deltix.timebase.connector.model.OwnerEntity"))
                .build();
        TimescaleColumn ownerName = TimescaleColumn.builder()
                .name("owner_name")
                .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                .relatedDescriptors(Collections.singletonList("deltix.timebase.connector.model.OwnerEntity"))
                .build();
        TimescaleColumn ownerPriority = TimescaleColumn.builder()
                .name("owner_priority")
                .dataType(TimescaleColumn.TimescaleDataType.INTEGER)
                .relatedDescriptors(Collections.singletonList("deltix.timebase.connector.model.OwnerEntity"))
                .build();
        TimescaleColumn quantity = TimescaleColumn.builder()
                .name("quantity")
                .dataType(TimescaleColumn.TimescaleDataType.DECIMAL64)
                .relatedDescriptors(Collections.singletonList("deltix.timebase.connector.model.DeltixEvent"))
                .build();
        TimescaleColumn reporter = TimescaleColumn.builder()
                .name("reporter")
                .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                .relatedDescriptors(Collections.singletonList("deltix.timebase.connector.model.DeltixEvent"))
                .build();
        TimescaleColumn dateTime = TimescaleColumn.builder()
                .name("dateTime")
                .dataType(TimescaleColumn.TimescaleDataType.DATETIME)
                .relatedDescriptors(Collections.singletonList("deltix.timebase.connector.model.BaseEvent"))
                .build();
        TimescaleColumn eventType = TimescaleColumn.builder()
                .name("eventType")
                .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                .relatedDescriptors(Collections.singletonList("deltix.timebase.connector.model.BaseEvent"))
                .build();
        TimescaleColumn finalColumn = TimescaleColumn.builder()
                .name("final")
                .dataType(TimescaleColumn.TimescaleDataType.BOOLEAN)
                .relatedDescriptors(Collections.singletonList("deltix.timebase.connector.model.BaseEvent"))
                .build();
        TimescaleColumn name = TimescaleColumn.builder()
                .name("name")
                .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                .relatedDescriptors(Collections.singletonList("deltix.timebase.connector.model.BaseEvent"))
                .build();
        TimescaleColumn global = TimescaleColumn.builder()
                .name("global")
                .dataType(TimescaleColumn.TimescaleDataType.BOOLEAN)
                .relatedDescriptors(Collections.singletonList("deltix.timebase.connector.model.TimebaseEvent"))
                .build();
        TimescaleColumn value = TimescaleColumn.builder()
                .name("value")
                .dataType(TimescaleColumn.TimescaleDataType.LONG)
                .relatedDescriptors(Collections.singletonList("deltix.timebase.connector.model.TimebaseEvent"))
                .build();

        assertThat(actualColumns.size(), is(16));
        assertThat(actualColumns, containsInAnyOrder(descriptorName, dateTime, eventType, finalColumn, global, name,
                ownerBusinessGroupDescriptorName, ownerBusinessGroupGroupId, ownerBusinessGroupGroupName,
                ownerDescriptorName, ownerEmail, ownerName, ownerPriority, quantity, reporter, value));
    }

    @Test
    public void testGetColumnsFromMessages() {
        ObjectArrayList<DataFieldInfo> firstDescriptorFields = new ObjectArrayList<>();
        deltix.timebase.messages.schema.NonStaticDataField nameField = new deltix.timebase.messages.schema.NonStaticDataField();
        nameField.setName("name");
        nameField.setDataType(new deltix.timebase.messages.schema.VarcharDataType());
        firstDescriptorFields.add(nameField);

        deltix.timebase.messages.schema.StaticDataField valueField = new deltix.timebase.messages.schema.StaticDataField();
        valueField.setStaticValue("default_value");
        valueField.setName("value");
        valueField.setTitle("title");
        valueField.setDataType(new deltix.timebase.messages.schema.VarcharDataType());
        firstDescriptorFields.add(valueField);


        deltix.timebase.messages.schema.RecordClassDescriptor descriptor1 = new deltix.timebase.messages.schema.RecordClassDescriptor();
        descriptor1.setIsAbstract(false);
        descriptor1.setParent(null);
        descriptor1.setTitle("title");
        descriptor1.setName("com.deltix.Descriptor1");
        descriptor1.setDataFields(firstDescriptorFields);

        ObjectArrayList<DataFieldInfo> secondDescriptorDataFields = new ObjectArrayList<>();
        deltix.timebase.messages.schema.NonStaticDataField priceField = new deltix.timebase.messages.schema.NonStaticDataField();
        priceField.setName("price");
        priceField.setTitle("title");
        priceField.setDataType(new deltix.timebase.messages.schema.FloatDataType());
        secondDescriptorDataFields.add(priceField);

        deltix.timebase.messages.schema.StaticDataField descriptionField = new deltix.timebase.messages.schema.StaticDataField();
        descriptionField.setStaticValue("default_value");
        descriptionField.setDataType(new deltix.timebase.messages.schema.VarcharDataType());
        descriptionField.setTitle("title");
        descriptionField.setName("description");
        secondDescriptorDataFields.add(descriptionField);

        deltix.timebase.messages.schema.ClassDataType attributeClassDataType = new deltix.timebase.messages.schema.ClassDataType();
        ObjectArrayList<ClassDescriptorRefInfo> refs = new ObjectArrayList<>();
        ClassDescriptorRef descriptorRef = new ClassDescriptorRef();
        descriptorRef.setName("com.deltix.Descriptor1");
        refs.add(descriptorRef);
        attributeClassDataType.setTypeDescriptors(refs);
        deltix.timebase.messages.schema.NonStaticDataField attributeField = new deltix.timebase.messages.schema.NonStaticDataField();
        attributeField.setName("attribute");
        attributeField.setTitle("title");
        attributeField.setDataType(attributeClassDataType);
        secondDescriptorDataFields.add(attributeField);

        deltix.timebase.messages.schema.RecordClassDescriptor descriptor2 = new deltix.timebase.messages.schema.RecordClassDescriptor();
        descriptor2.setParent(null);
        descriptor2.setIsAbstract(false);
        descriptor2.setTitle("title");
        descriptor2.setName("com.deltix.Descriptor2");
        descriptor2.setDataFields(secondDescriptorDataFields);

        ObjectArrayList<DataFieldInfo> thirdDescriptorFields = new ObjectArrayList<>();

        deltix.timebase.messages.schema.NonStaticDataField typeField = new deltix.timebase.messages.schema.NonStaticDataField();
        typeField.setName("type");
        typeField.setTitle("title");
        typeField.setDataType(new deltix.timebase.messages.schema.VarcharDataType());
        thirdDescriptorFields.add(typeField);

        deltix.timebase.messages.schema.IntegerDataType integerDataType = new deltix.timebase.messages.schema.IntegerDataType();
        integerDataType.setEncoding("INT64");
        deltix.timebase.messages.schema.NonStaticDataField codeField = new deltix.timebase.messages.schema.NonStaticDataField();
        codeField.setDataType(integerDataType);
        codeField.setTitle("title");
        codeField.setName("code");
        thirdDescriptorFields.add(codeField);

        deltix.timebase.messages.schema.ClassDataType eventClassDataType = new deltix.timebase.messages.schema.ClassDataType();
        ObjectArrayList<ClassDescriptorRefInfo> refs1 = new ObjectArrayList<>();
        ClassDescriptorRef descriptorRef1 = new ClassDescriptorRef();
        descriptorRef1.setName("com.deltix.Descriptor2");
        refs1.add(descriptorRef1);
        eventClassDataType.setTypeDescriptors(refs1);
        deltix.timebase.messages.schema.NonStaticDataField eventField = new deltix.timebase.messages.schema.NonStaticDataField();
        eventField.setName("event");
        eventField.setTitle("title");
        eventField.setDataType(eventClassDataType);
        thirdDescriptorFields.add(eventField);

        deltix.timebase.messages.schema.RecordClassDescriptor descriptor3 = new deltix.timebase.messages.schema.RecordClassDescriptor();
        descriptor3.setParent(null);
        descriptor3.setIsAbstract(false);
        descriptor3.setTitle("title");
        descriptor3.setName("com.deltix.Descriptor3");
        descriptor3.setDataFields(thirdDescriptorFields);

        List<TimescaleColumn> actualColumns = descriptorService.getColumnsFromMessages(Arrays.asList(descriptor3, descriptor1, descriptor2));

        List<TimescaleColumn> expectedColumns = getExpectedColumns();

        assertThat(actualColumns.size(), is(expectedColumns.size()));
        assertThat(actualColumns, containsInAnyOrder(expectedColumns.toArray(new TimescaleColumn[expectedColumns.size()])));
    }

    private List<TimescaleColumn> getExpectedColumns() {
        List<TimescaleColumn> columns = new ArrayList<>();
        columns.add(
                TimescaleColumn.builder()
                        .name("descriptor_name")
                        .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                        .relatedDescriptors(Collections.singletonList("com.deltix.Descriptor3"))
                        .build()
        );
        columns.add(
                TimescaleColumn.builder()
                        .name("type")
                        .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                        .relatedDescriptors(Collections.singletonList("com.deltix.Descriptor3"))
                        .build()
        );
        columns.add(
                TimescaleColumn.builder()
                        .name("code")
                        .dataType(TimescaleColumn.TimescaleDataType.LONG)
                        .relatedDescriptors(Collections.singletonList("com.deltix.Descriptor3"))
                        .build()
        );
        columns.add(
                TimescaleColumn.builder()
                        .name("event_descriptor_name")
                        .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                        .relatedDescriptors(Collections.singletonList("com.deltix.Descriptor2"))
                        .build()
        );
        columns.add(
                TimescaleColumn.builder()
                        .name("event_description")
                        .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                        .relatedDescriptors(Collections.singletonList("com.deltix.Descriptor2"))
                        .build()
        );
        columns.add(
                TimescaleColumn.builder()
                        .name("event_price")
                        .dataType(TimescaleColumn.TimescaleDataType.DECIMAL)
                        .relatedDescriptors(Collections.singletonList("com.deltix.Descriptor2"))
                        .build()
        );
        columns.add(
                TimescaleColumn.builder()
                        .name("event_attribute_descriptor_name")
                        .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                        .relatedDescriptors(Collections.singletonList("com.deltix.Descriptor1"))
                        .build()
        );
        columns.add(
                TimescaleColumn.builder()
                        .name("event_attribute_name")
                        .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                        .relatedDescriptors(Collections.singletonList("com.deltix.Descriptor1"))
                        .build()
        );
        columns.add(
                TimescaleColumn.builder()
                        .name("event_attribute_value")
                        .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                        .relatedDescriptors(Collections.singletonList("com.deltix.Descriptor1"))
                        .build()
        );

        return columns;
    }
}

package deltix.timebase.connector.service.timescale;

import deltix.qsrv.hf.pub.md.Introspector;
import deltix.timebase.connector.model.FirstTestClass;
import deltix.timebase.connector.model.MigrationMetadata;
import deltix.timebase.connector.model.SecondTestClass;
import deltix.timebase.connector.model.schema.TimescaleSchema;
import deltix.timebase.connector.service.BaseServiceIntegrationTest;
import deltix.timebase.connector.service.MigrationService;
import deltix.timebase.messages.schema.*;
import deltix.util.collections.generated.ObjectArrayList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

public class TimescaleMigrationServiceTest extends BaseServiceIntegrationTest {

    @Autowired
    private MigrationService<SchemaChangeMessage> migrationService;

    @Autowired
    private TimescaleSqlGenerator sqlGenerator;

    @Autowired
    private TimescaleDataService dataService;

    @Autowired
    private TimescaleSchemaDefinition schemaDefinition;

    @MockBean
    private TimescaleMigrationMetadataService migrationMetadataService;

    @BeforeEach
    public void mock() {
        when(migrationMetadataService.getByStreamName(any(String.class))).thenReturn(Optional.of(MigrationMetadata.builder().build()));
        doNothing().when(migrationMetadataService).update(any(MigrationMetadata.class));
    }

    @Test
    public void testAddDescriptorMigration() {
        applyTimescaleSchema();

        SchemaChangeMessage changeMessage = new SchemaChangeMessage();
        changeMessage.setPreviousState(getDescriptorMessages());
        SchemaDescriptorChangeAction addDescriptorAction = new SchemaDescriptorChangeAction();
        addDescriptorAction.setChangeTypes(SchemaDescriptorChangeType.ADD);

        RecordClassDescriptor recordClassDescriptor = new RecordClassDescriptor();
        recordClassDescriptor.setName("org.some.package.ThirdDescriptor");
        DataField firstField = new NonStaticDataField();
        firstField.setName("title");
        firstField.setDataType(new VarcharDataType());

        DataField secondField = new NonStaticDataField();
        secondField.setName("coast");
        secondField.setDataType(new FloatDataType());

        ObjectArrayList<DataFieldInfo> fields = new ObjectArrayList<>();
        fields.add(firstField);
        fields.add(secondField);

        recordClassDescriptor.setDataFields(fields);

        addDescriptorAction.setNewState(recordClassDescriptor);

        ObjectArrayList<ClassDescriptorInfo> newState = getDescriptorMessages();
        newState.add(recordClassDescriptor);

        changeMessage.setNewState(newState);

        ObjectArrayList<SchemaDescriptorChangeActionInfo> actions = new ObjectArrayList<>();
        actions.add(addDescriptorAction);

        changeMessage.setDescriptorChangeActions(actions);

        migrationService.apply(changeMessage, "events");
    }

    @Test
    public void testRemoveDescriptorMigration() {
        applyTimescaleSchema();

        SchemaChangeMessage changeMessage = new SchemaChangeMessage();
        SchemaDescriptorChangeAction addDescriptorAction = new SchemaDescriptorChangeAction();
        addDescriptorAction.setChangeTypes(SchemaDescriptorChangeType.DELETE);

        ObjectArrayList<ClassDescriptorInfo> previousState = getDescriptorMessages();
        ObjectArrayList<ClassDescriptorInfo> newState = previousState.stream()
                .filter(d -> !d.getName().equals("deltix.timebase.connector.model.SecondTestClass"))
                .collect(Collectors.toCollection(ObjectArrayList::new));

        changeMessage.setPreviousState(previousState);
        changeMessage.setNewState(newState);

        ClassDescriptorInfo descriptorToRemove = previousState.stream()
                .filter(d -> d.getName().equals("deltix.timebase.connector.model.SecondTestClass"))
                .findAny()
                .get();

        addDescriptorAction.setPreviousState(descriptorToRemove);

        ObjectArrayList<SchemaDescriptorChangeActionInfo> actions = new ObjectArrayList<>();
        actions.add(addDescriptorAction);

        changeMessage.setDescriptorChangeActions(actions);

        migrationService.apply(changeMessage, "events");
    }

    @Test
    public void testRenameDescriptorMigration() {
        applyTimescaleSchema();

        SchemaChangeMessage changeMessage = new SchemaChangeMessage();
        SchemaDescriptorChangeAction renameDescriptorAction = new SchemaDescriptorChangeAction();
        renameDescriptorAction.setChangeTypes(SchemaDescriptorChangeType.RENAME);

        ClassDescriptorInfo previousDescriptor = null;
        ObjectArrayList<ClassDescriptorInfo> previousState = getDescriptorMessages();
        for (ClassDescriptorInfo d : previousState) {
            if (d.getName().equals("deltix.timebase.connector.model.SecondTestClass"))
                previousDescriptor = d;
        }

        ClassDescriptorInfo newDescriptor = null;
        ObjectArrayList<ClassDescriptorInfo> newState = getDescriptorMessages();
        for (ClassDescriptorInfo d : newState) {
            if (d.getName().equals("deltix.timebase.connector.model.SecondTestClass")) {
                ((RecordClassDescriptor) d).setName("deltix.timebase.connector.model.RenamedSecondTestClass");
                newDescriptor = d;
            }
        }

        changeMessage.setNewState(newState);
        changeMessage.setPreviousState(previousState);

        renameDescriptorAction.setPreviousState(previousDescriptor);
        renameDescriptorAction.setNewState(newDescriptor);

        ObjectArrayList<SchemaDescriptorChangeActionInfo> actions = new ObjectArrayList<>();
        actions.add(renameDescriptorAction);

        changeMessage.setDescriptorChangeActions(actions);

        migrationService.apply(changeMessage, "events");
    }

    @Test
    public void testAddNewColumnToExistedDescriptor() {
        applyTimescaleSchema();

        SchemaChangeMessage changeMessage = new SchemaChangeMessage();
        SchemaDescriptorChangeAction alterDescriptorAction = new SchemaDescriptorChangeAction();
        alterDescriptorAction.setChangeTypes(SchemaDescriptorChangeType.FIELDS_CHANGE);

        ClassDescriptorInfo previousDescriptor = null;
        ObjectArrayList<ClassDescriptorInfo> previousState = getDescriptorMessages();
        for (ClassDescriptorInfo d : previousState) {
            if (d.getName().equals("deltix.timebase.connector.model.SecondTestClass"))
                previousDescriptor = d;
        }

        DataField newDataField = new NonStaticDataField();
        newDataField.setName("newField");
        newDataField.setDataType(new VarcharDataType());

        ClassDescriptorInfo newDescriptor = null;
        ObjectArrayList<ClassDescriptorInfo> newState = getDescriptorMessages();
        for (ClassDescriptorInfo d : newState) {
            if (d.getName().equals("deltix.timebase.connector.model.SecondTestClass")) {
                RecordClassDescriptor rcd = (RecordClassDescriptor) d;

                rcd.getDataFields().add(newDataField);
                newDescriptor = rcd;
            }
        }

        changeMessage.setNewState(newState);
        changeMessage.setPreviousState(previousState);

        alterDescriptorAction.setPreviousState(previousDescriptor);
        alterDescriptorAction.setNewState(newDescriptor);

        ObjectArrayList<SchemaFieldChangeActionInfo> filedActions = new ObjectArrayList<>();
        SchemaFieldChangeAction filedAction = new SchemaFieldChangeAction();
        filedAction.setChangeTypes(SchemaFieldChangeType.ADD);
        filedAction.setNewState(newDataField);

        filedActions.add(filedAction);

        alterDescriptorAction.setFieldChangeActions(filedActions);

        ObjectArrayList<SchemaDescriptorChangeActionInfo> actions = new ObjectArrayList<>();
        actions.add(alterDescriptorAction);

        changeMessage.setDescriptorChangeActions(actions);

        migrationService.apply(changeMessage, "events");
    }

    @Test
    public void testDropColumnInExistedDescriptor() {
        applyTimescaleSchema();

        SchemaChangeMessage changeMessage = new SchemaChangeMessage();
        SchemaDescriptorChangeAction alterDescriptorAction = new SchemaDescriptorChangeAction();
        alterDescriptorAction.setChangeTypes(SchemaDescriptorChangeType.FIELDS_CHANGE);

        ClassDescriptorInfo previousDescriptor = null;
        ObjectArrayList<ClassDescriptorInfo> previousState = getDescriptorMessages();
        for (ClassDescriptorInfo d : previousState) {
            if (d.getName().equals("deltix.timebase.connector.model.SecondTestClass"))
                previousDescriptor = d;
        }

        DataFieldInfo dataFieldToDelete = null;
        ClassDescriptorInfo newDescriptor = null;
        ObjectArrayList<ClassDescriptorInfo> newState = getDescriptorMessages();
        for (ClassDescriptorInfo d : newState) {
            if (d.getName().equals("deltix.timebase.connector.model.SecondTestClass")) {
                RecordClassDescriptor rcd = (RecordClassDescriptor) d;

                for (DataFieldInfo field : rcd.getDataFields()) {
                    if (field.getName().equals("secondLongValue")) {
                        dataFieldToDelete = field;
                    }
                }
                rcd.getDataFields().remove(dataFieldToDelete);
                newDescriptor = rcd;
            }
        }

        changeMessage.setNewState(newState);
        changeMessage.setPreviousState(previousState);

        alterDescriptorAction.setPreviousState(previousDescriptor);
        alterDescriptorAction.setNewState(newDescriptor);

        ObjectArrayList<SchemaFieldChangeActionInfo> filedActions = new ObjectArrayList<>();
        SchemaFieldChangeAction filedAction = new SchemaFieldChangeAction();
        filedAction.setChangeTypes(SchemaFieldChangeType.DELETE);
        filedAction.setPreviousState(dataFieldToDelete);

        filedActions.add(filedAction);

        alterDescriptorAction.setFieldChangeActions(filedActions);

        ObjectArrayList<SchemaDescriptorChangeActionInfo> actions = new ObjectArrayList<>();
        actions.add(alterDescriptorAction);

        changeMessage.setDescriptorChangeActions(actions);

        migrationService.apply(changeMessage, "events");
    }

    @Test
    public void testRenameColumnInExistedDescriptor() {
        applyTimescaleSchema();

        SchemaChangeMessage changeMessage = new SchemaChangeMessage();
        SchemaDescriptorChangeAction alterDescriptorAction = new SchemaDescriptorChangeAction();
        alterDescriptorAction.setChangeTypes(SchemaDescriptorChangeType.FIELDS_CHANGE);

        DataFieldInfo previousField = null;
        ClassDescriptorInfo previousDescriptor = null;
        ObjectArrayList<ClassDescriptorInfo> previousState = getDescriptorMessages();
        for (ClassDescriptorInfo d : previousState) {
            if (d.getName().equals("deltix.timebase.connector.model.SecondTestClass")) {
                RecordClassDescriptor rcd = (RecordClassDescriptor) d;

                for (DataFieldInfo field : rcd.getDataFields()) {
                    if (field.getName().equals("secondLongValue")) {
                        previousField = field;
                    }
                }
                previousDescriptor = rcd;
            }
        }

        DataFieldInfo renamedDataField = null;
        ClassDescriptorInfo newDescriptor = null;
        ObjectArrayList<ClassDescriptorInfo> newState = getDescriptorMessages();
        for (ClassDescriptorInfo d : newState) {
            if (d.getName().equals("deltix.timebase.connector.model.SecondTestClass")) {
                RecordClassDescriptor rcd = (RecordClassDescriptor) d;

                for (DataFieldInfo field : rcd.getDataFields()) {
                    if (field.getName().equals("secondLongValue")) {
                        renamedDataField = field;
                        ((DataField) renamedDataField).setName("renamedSecondLongValue");
                    }
                }
                newDescriptor = rcd;
            }
        }

        changeMessage.setNewState(newState);
        changeMessage.setPreviousState(previousState);

        alterDescriptorAction.setPreviousState(previousDescriptor);
        alterDescriptorAction.setNewState(newDescriptor);

        ObjectArrayList<SchemaFieldChangeActionInfo> filedActions = new ObjectArrayList<>();
        SchemaFieldChangeAction filedAction = new SchemaFieldChangeAction();
        filedAction.setChangeTypes(SchemaFieldChangeType.RENAME);
        filedAction.setNewState(renamedDataField);
        filedAction.setPreviousState(previousField);

        filedActions.add(filedAction);

        alterDescriptorAction.setFieldChangeActions(filedActions);

        ObjectArrayList<SchemaDescriptorChangeActionInfo> actions = new ObjectArrayList<>();
        actions.add(alterDescriptorAction);

        changeMessage.setDescriptorChangeActions(actions);

        migrationService.apply(changeMessage, "events");
    }

    @Test
    public void testChangeStaticValue() {
        applyTimescaleSchema();

        SchemaChangeMessage changeMessage = new SchemaChangeMessage();
        SchemaDescriptorChangeAction alterDescriptorAction = new SchemaDescriptorChangeAction();
        alterDescriptorAction.setChangeTypes(SchemaDescriptorChangeType.FIELDS_CHANGE);

        DataFieldInfo previousFieldState = null;
        ClassDescriptorInfo previousDescriptor = null;
        ObjectArrayList<ClassDescriptorInfo> previousState = getDescriptorMessages();
        for (ClassDescriptorInfo d : previousState) {
            if (d.getName().equals("deltix.timebase.connector.model.SecondTestClass")) {
                RecordClassDescriptor rcd = (RecordClassDescriptor) d;

                for (DataFieldInfo field : rcd.getDataFields()) {
                    if (field.getName().equals("secondLongValue")) {
                        previousFieldState = field;
                    }
                }
                previousDescriptor = rcd;
            }
        }

        DataFieldInfo newFieldState = null;
        ClassDescriptorInfo newDescriptor = null;
        ObjectArrayList<ClassDescriptorInfo> newState = getDescriptorMessages();
        for (ClassDescriptorInfo d : newState) {
            if (d.getName().equals("deltix.timebase.connector.model.SecondTestClass")) {
                RecordClassDescriptor rcd = (RecordClassDescriptor) d;

                for (DataFieldInfo field : rcd.getDataFields()) {
                    if (field.getName().equals("secondLongValue")) {
                        newFieldState = field;
                    }
                }
                newDescriptor = rcd;
            }
        }

        changeMessage.setNewState(newState);
        changeMessage.setPreviousState(previousState);

        alterDescriptorAction.setPreviousState(previousDescriptor);
        alterDescriptorAction.setNewState(newDescriptor);

        ObjectArrayList<SchemaFieldChangeActionInfo> filedActions = new ObjectArrayList<>();
        SchemaFieldChangeAction filedAction = new SchemaFieldChangeAction();
        filedAction.setChangeTypes(SchemaFieldChangeType.STATIC_VALUE_CHANGE);
        filedAction.setNewState(newFieldState);
        filedAction.setPreviousState(previousFieldState);

        SchemaFieldDataTransformation filedTransformation = new SchemaFieldDataTransformation();
        filedTransformation.setDefaultValue("1237454");

        filedAction.setDataTransformation(filedTransformation);

        filedActions.add(filedAction);

        alterDescriptorAction.setFieldChangeActions(filedActions);

        ObjectArrayList<SchemaDescriptorChangeActionInfo> actions = new ObjectArrayList<>();
        actions.add(alterDescriptorAction);

        changeMessage.setDescriptorChangeActions(actions);

        migrationService.apply(changeMessage, "events");
    }

    @Test
    public void testChangeDataType() {
        applyTimescaleSchema();

        SchemaChangeMessage changeMessage = new SchemaChangeMessage();
        SchemaDescriptorChangeAction alterDescriptorAction = new SchemaDescriptorChangeAction();
        alterDescriptorAction.setChangeTypes(SchemaDescriptorChangeType.FIELDS_CHANGE);

        DataFieldInfo previousFieldState = null;
        ClassDescriptorInfo previousDescriptor = null;
        ObjectArrayList<ClassDescriptorInfo> previousState = getDescriptorMessages();
        for (ClassDescriptorInfo d : previousState) {
            if (d.getName().equals("deltix.timebase.connector.model.SecondTestClass")) {
                RecordClassDescriptor rcd = (RecordClassDescriptor) d;

                for (DataFieldInfo field : rcd.getDataFields()) {
                    if (field.getName().equals("secondLongValue")) {
                        previousFieldState = field;
                    }
                }
                previousDescriptor = rcd;
            }
        }

        DataFieldInfo newFieldState = null;
        ClassDescriptorInfo newDescriptor = null;
        ObjectArrayList<ClassDescriptorInfo> newState = getDescriptorMessages();
        for (ClassDescriptorInfo d : newState) {
            if (d.getName().equals("deltix.timebase.connector.model.SecondTestClass")) {
                RecordClassDescriptor rcd = (RecordClassDescriptor) d;

                for (DataFieldInfo field : rcd.getDataFields()) {
                    if (field.getName().equals("secondLongValue")) {
                        ((DataField) field).setDataType(new VarcharDataType());
                        newFieldState = field;
                    }
                }
                newDescriptor = rcd;
            }
        }

        changeMessage.setNewState(newState);
        changeMessage.setPreviousState(previousState);


        alterDescriptorAction.setPreviousState(previousDescriptor);
        alterDescriptorAction.setNewState(newDescriptor);

        ObjectArrayList<SchemaFieldChangeActionInfo> filedActions = new ObjectArrayList<>();
        SchemaFieldChangeAction filedAction = new SchemaFieldChangeAction();
        filedAction.setChangeTypes(SchemaFieldChangeType.DATA_TYPE_CHANGE);
        filedAction.setNewState(newFieldState);
        filedAction.setPreviousState(previousFieldState);

        SchemaFieldDataTransformation filedTransformation = new SchemaFieldDataTransformation();
        filedTransformation.setTransformationType(SchemaFieldDataTransformationType.CONVERT_DATA);

        filedAction.setDataTransformation(filedTransformation);

        filedActions.add(filedAction);

        alterDescriptorAction.setFieldChangeActions(filedActions);

        ObjectArrayList<SchemaDescriptorChangeActionInfo> actions = new ObjectArrayList<>();
        actions.add(alterDescriptorAction);

        changeMessage.setDescriptorChangeActions(actions);

        migrationService.apply(changeMessage, "events");
    }

    @Test
    public void testSetDefaultValue() {
        applyTimescaleSchema();

        SchemaChangeMessage changeMessage = new SchemaChangeMessage();
        SchemaDescriptorChangeAction alterDescriptorAction = new SchemaDescriptorChangeAction();
        alterDescriptorAction.setChangeTypes(SchemaDescriptorChangeType.FIELDS_CHANGE);

        DataFieldInfo previousFieldState = null;
        ClassDescriptorInfo previousDescriptor = null;
        ObjectArrayList<ClassDescriptorInfo> previousState = getDescriptorMessages();
        for (ClassDescriptorInfo d : previousState) {
            if (d.getName().equals("deltix.timebase.connector.model.SecondTestClass")) {
                RecordClassDescriptor rcd = (RecordClassDescriptor) d;

                for (DataFieldInfo field : rcd.getDataFields()) {
                    if (field.getName().equals("secondLongValue")) {
                        previousFieldState = field;
                    }
                }
                previousDescriptor = rcd;
            }
        }

        DataFieldInfo newFieldState = null;
        ClassDescriptorInfo newDescriptor = null;
        ObjectArrayList<ClassDescriptorInfo> newState = getDescriptorMessages();
        for (ClassDescriptorInfo d : newState) {
            if (d.getName().equals("deltix.timebase.connector.model.SecondTestClass")) {
                RecordClassDescriptor rcd = (RecordClassDescriptor) d;

                for (DataFieldInfo field : rcd.getDataFields()) {
                    if (field.getName().equals("secondLongValue")) {
                        newFieldState = field;
                    }
                }
                newDescriptor = rcd;
            }
        }

        changeMessage.setNewState(newState);
        changeMessage.setPreviousState(previousState);

        alterDescriptorAction.setPreviousState(previousDescriptor);
        alterDescriptorAction.setNewState(newDescriptor);

        ObjectArrayList<SchemaFieldChangeActionInfo> filedActions = new ObjectArrayList<>();
        SchemaFieldChangeAction filedAction = new SchemaFieldChangeAction();
        filedAction.setChangeTypes(SchemaFieldChangeType.DATA_TYPE_CHANGE);
        filedAction.setNewState(newFieldState);
        filedAction.setPreviousState(previousFieldState);

        SchemaFieldDataTransformation filedTransformation = new SchemaFieldDataTransformation();
        filedTransformation.setTransformationType(SchemaFieldDataTransformationType.SET_DEFAULT);
        filedTransformation.setDefaultValue("1274545");

        filedAction.setDataTransformation(filedTransformation);

        filedActions.add(filedAction);

        alterDescriptorAction.setFieldChangeActions(filedActions);

        ObjectArrayList<SchemaDescriptorChangeActionInfo> actions = new ObjectArrayList<>();
        actions.add(alterDescriptorAction);

        changeMessage.setDescriptorChangeActions(actions);

        migrationService.apply(changeMessage, "events");
    }

    @Test
    public void testNonStaticChangeModifier() {
        applyTimescaleSchema();

        SchemaChangeMessage changeMessage = new SchemaChangeMessage();
        SchemaDescriptorChangeAction alterDescriptorAction = new SchemaDescriptorChangeAction();
        alterDescriptorAction.setChangeTypes(SchemaDescriptorChangeType.FIELDS_CHANGE);

        DataFieldInfo previousFieldState = null;
        ClassDescriptorInfo previousDescriptor = null;
        ObjectArrayList<ClassDescriptorInfo> previousState = getDescriptorMessages();
        for (ClassDescriptorInfo d : previousState) {
            if (d.getName().equals("deltix.timebase.connector.model.SecondTestClass")) {
                RecordClassDescriptor rcd = (RecordClassDescriptor) d;

                for (DataFieldInfo field : rcd.getDataFields()) {
                    if (field.getName().equals("secondLongValue")) {
                        previousFieldState = field;
                    }
                }
                previousDescriptor = rcd;
            }
        }

        IntegerDataType longDataType = new IntegerDataType();
        StaticDataField newField = new StaticDataField();
        newField.setName("secondLongValue");
        newField.setStaticValue("1237454");
        newField.setDataType(longDataType);

        DataFieldInfo newFieldState = null;
        ClassDescriptorInfo newDescriptor = null;
        ObjectArrayList<ClassDescriptorInfo> newState = getDescriptorMessages();
        for (ClassDescriptorInfo d : newState) {
            if (d.getName().equals("deltix.timebase.connector.model.SecondTestClass")) {
                RecordClassDescriptor rcd = (RecordClassDescriptor) d;

                for (DataFieldInfo field : rcd.getDataFields()) {
                    if (field.getName().equals("secondLongValue")) {
                        newFieldState = field;
                    }
                }
                rcd.getDataFields().remove(newFieldState);
                newFieldState = newField;
                rcd.getDataFields().add(newFieldState);
                newDescriptor = rcd;
            }
        }

        changeMessage.setNewState(newState);
        changeMessage.setPreviousState(previousState);

        alterDescriptorAction.setPreviousState(previousDescriptor);
        alterDescriptorAction.setNewState(newDescriptor);

        ObjectArrayList<SchemaFieldChangeActionInfo> filedActions = new ObjectArrayList<>();
        SchemaFieldChangeAction filedAction = new SchemaFieldChangeAction();
        filedAction.setChangeTypes(SchemaFieldChangeType.MODIFIER_CHANGE);
        filedAction.setNewState(newFieldState);
        filedAction.setPreviousState(previousFieldState);

        SchemaFieldDataTransformation filedTransformation = new SchemaFieldDataTransformation();
        filedTransformation.setDefaultValue("1237454");

        filedAction.setDataTransformation(filedTransformation);

        filedActions.add(filedAction);

        alterDescriptorAction.setFieldChangeActions(filedActions);

        ObjectArrayList<SchemaDescriptorChangeActionInfo> actions = new ObjectArrayList<>();
        actions.add(alterDescriptorAction);

        changeMessage.setDescriptorChangeActions(actions);

        migrationService.apply(changeMessage, "events");
    }

    @Test
    public void testDropRecord() {
        applyTimescaleSchema();

        SchemaChangeMessage changeMessage = new SchemaChangeMessage();
        SchemaDescriptorChangeAction alterDescriptorAction = new SchemaDescriptorChangeAction();
        alterDescriptorAction.setChangeTypes(SchemaDescriptorChangeType.FIELDS_CHANGE);

        DataFieldInfo previousFieldState = null;
        ClassDescriptorInfo previousDescriptor = null;
        ObjectArrayList<ClassDescriptorInfo> previousState = getDescriptorMessages();
        for (ClassDescriptorInfo d : previousState) {
            if (d.getName().equals("deltix.timebase.connector.model.SecondTestClass")) {
                RecordClassDescriptor rcd = (RecordClassDescriptor) d;

                for (DataFieldInfo field : rcd.getDataFields()) {
                    if (field.getName().equals("secondLongValue")) {
                        previousFieldState = field;
                    }
                }
                previousDescriptor = rcd;
            }
        }

        DataFieldInfo newFieldState = null;
        ClassDescriptorInfo newDescriptor = null;
        ObjectArrayList<ClassDescriptorInfo> newState = getDescriptorMessages();
        for (ClassDescriptorInfo d : newState) {
            if (d.getName().equals("deltix.timebase.connector.model.SecondTestClass")) {
                RecordClassDescriptor rcd = (RecordClassDescriptor) d;

                for (DataFieldInfo field : rcd.getDataFields()) {
                    if (field.getName().equals("secondLongValue")) {
                        newFieldState = field;
                    }
                }
                newDescriptor = rcd;
            }
        }

        changeMessage.setNewState(newState);
        changeMessage.setPreviousState(previousState);


        alterDescriptorAction.setPreviousState(previousDescriptor);
        alterDescriptorAction.setNewState(newDescriptor);

        ObjectArrayList<SchemaFieldChangeActionInfo> filedActions = new ObjectArrayList<>();
        SchemaFieldChangeAction filedAction = new SchemaFieldChangeAction();
        filedAction.setChangeTypes(SchemaFieldChangeType.DATA_TYPE_CHANGE);
        filedAction.setNewState(newFieldState);
        filedAction.setPreviousState(previousFieldState);

        SchemaFieldDataTransformation filedTransformation = new SchemaFieldDataTransformation();
        filedTransformation.setTransformationType(SchemaFieldDataTransformationType.DROP_RECORD);

        filedAction.setDataTransformation(filedTransformation);

        filedActions.add(filedAction);

        alterDescriptorAction.setFieldChangeActions(filedActions);

        ObjectArrayList<SchemaDescriptorChangeActionInfo> actions = new ObjectArrayList<>();
        actions.add(alterDescriptorAction);

        changeMessage.setDescriptorChangeActions(actions);

        migrationService.apply(changeMessage, "events");
    }

    private void applyTimescaleSchema() {
        TimescaleSchema schema = getTimescaleSchema();
        String createTableStatement = sqlGenerator.generateCreateTableStatement(schema);
        String hypertableStatement = sqlGenerator.generateHypertableStatement(schema);

        dataService.executeQuery(createTableStatement);
        dataService.executeQuery(hypertableStatement);
    }

    private TimescaleSchema getTimescaleSchema() {
        Introspector introspector = Introspector.createEmptyMessageIntrospector();
        try {
            introspector.introspectRecordClass(FirstTestClass.class);
            introspector.introspectRecordClass(SecondTestClass.class);

        } catch (Introspector.IntrospectionException ex) {
            ex.printStackTrace();
        }

        return schemaDefinition.getTimebaseSchemaDefinition(introspector.getAllClasses(), "events");
    }

    private ObjectArrayList<ClassDescriptorInfo> getDescriptorMessages() {
        ObjectArrayList<ClassDescriptorInfo> descriptors = new ObjectArrayList<>();

        ObjectArrayList<DataFieldInfo> innerTestFields = new ObjectArrayList<>();

        DataField nameField = new NonStaticDataField();
        nameField.setDataType(new VarcharDataType());
        nameField.setName("name");

        DataField valueField = new NonStaticDataField();
        valueField.setName("value");
        valueField.setDataType(new VarcharDataType());

        innerTestFields.addAll(Arrays.asList(valueField, nameField));

        RecordClassDescriptor innerTestDescriptor = new RecordClassDescriptor();
        innerTestDescriptor.setName("deltix.timebase.connector.model.InnerTestClass");
        innerTestDescriptor.setIsAbstract(false);
        innerTestDescriptor.setTitle("title");
        innerTestDescriptor.setParent(null);
        innerTestDescriptor.setDataFields(innerTestFields);

        EnumClassDescriptor someTestEnumDescriptor = new EnumClassDescriptor();
        someTestEnumDescriptor.setName("deltix.timebase.connector.model.SomeTestEnum");
        someTestEnumDescriptor.setTitle("title");

        ObjectArrayList<DataFieldInfo> baseTestClassFields = new ObjectArrayList<>();

        DataField stringValueField = new NonStaticDataField();
        stringValueField.setName("stringValue");
        stringValueField.setDataType(new VarcharDataType());

        FloatDataType decimal64DataType = new FloatDataType();
        decimal64DataType.setEncoding("DECIMAL64");
        DataField decimal64Field = new NonStaticDataField();
        decimal64Field.setName("decimal64Value");
        decimal64Field.setDataType(decimal64DataType);

        DataField byteaField = new NonStaticDataField();
        byteaField.setDataType(new BinaryDataType());
        byteaField.setName("byteaValue");

        baseTestClassFields.addAll(Arrays.asList(stringValueField, decimal64Field, byteaField));

        RecordClassDescriptor baseTestClassDescriptor = new RecordClassDescriptor();
        baseTestClassDescriptor.setParent(null);
        baseTestClassDescriptor.setIsAbstract(true);
        baseTestClassDescriptor.setName("deltix.timebase.connector.model.BaseTestClass");
        baseTestClassDescriptor.setTitle("title");
        baseTestClassDescriptor.setDataFields(baseTestClassFields);

        ObjectArrayList<DataFieldInfo> firstTestClassFields = new ObjectArrayList<>();

        DataField charField = new NonStaticDataField();
        charField.setName("charValue");
        charField.setDataType(new CharDataType());

        IntegerDataType integerDataType = new IntegerDataType();
        integerDataType.setEncoding("INT32");
        DataField intField = new NonStaticDataField();
        intField.setDataType(integerDataType);
        intField.setName("intValue");

        IntegerDataType longDataType = new IntegerDataType();
        DataField longField = new NonStaticDataField();
        longField.setDataType(longDataType);
        longField.setName("longValue");

        ClassDataType classDataType = new ClassDataType();
        ObjectArrayList<ClassDescriptorRefInfo> descriptorRefs = new ObjectArrayList<>();
        ClassDescriptorRef descriptorRef = new ClassDescriptorRef();
        descriptorRef.setName("deltix.timebase.connector.model.InnerTestClass");
        descriptorRefs.add(descriptorRef);
        classDataType.setTypeDescriptors(descriptorRefs);
        DataField classField = new NonStaticDataField();
        classField.setName("innerState");
        charField.setDataType(classDataType);

        firstTestClassFields.addAll(Arrays.asList(charField, intField, longField, classField));

        RecordClassDescriptor firstTestClassDescriptor = new RecordClassDescriptor();
        firstTestClassDescriptor.setParent(baseTestClassDescriptor);
        firstTestClassDescriptor.setIsAbstract(false);
        firstTestClassDescriptor.setName("deltix.timebase.connector.model.FirstTestClass");
        firstTestClassDescriptor.setDataFields(firstTestClassFields);

        ObjectArrayList<DataFieldInfo> secondTestClassFields = new ObjectArrayList<>();

        DataField secondValue1Field = new NonStaticDataField();
        secondValue1Field.setName("secondValue1");
        secondValue1Field.setDataType(new VarcharDataType());

        DataField secondLongValueField = new NonStaticDataField();
        secondLongValueField.setDataType(longDataType);
        secondLongValueField.setName("secondLongValue");

        EnumDataType enumDataType = new EnumDataType();
        ClassDescriptorRef enumDescriptorRef = new ClassDescriptorRef();
        enumDescriptorRef.setName("deltix.timebase.connector.model.SomeTestEnum");
        enumDataType.setTypeDescriptor(enumDescriptorRef);
        DataField enumField = new NonStaticDataField();
        enumField.setName("enumValue");
        enumField.setDataType(enumDataType);

        secondTestClassFields.addAll(Arrays.asList(enumField, secondLongValueField, secondValue1Field));

        RecordClassDescriptor secondRecordClassDescriptor = new RecordClassDescriptor();
        secondRecordClassDescriptor.setName("deltix.timebase.connector.model.SecondTestClass");
        secondRecordClassDescriptor.setIsAbstract(false);
        secondRecordClassDescriptor.setParent(baseTestClassDescriptor);
        secondRecordClassDescriptor.setDataFields(secondTestClassFields);

        descriptors.addAll(Arrays.asList(baseTestClassDescriptor, secondRecordClassDescriptor, firstTestClassDescriptor,
                someTestEnumDescriptor, innerTestDescriptor));

        return descriptors;
    }
}

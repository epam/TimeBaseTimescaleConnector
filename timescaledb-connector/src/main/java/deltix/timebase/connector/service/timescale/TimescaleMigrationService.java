package deltix.timebase.connector.service.timescale;

import deltix.gflog.Log;
import deltix.gflog.LogFactory;
import deltix.timebase.connector.exception.ColumnNotFoundException;
import deltix.timebase.connector.model.MigrationMetadata;
import deltix.timebase.connector.model.schema.TimescaleColumn;
import deltix.timebase.connector.model.schema.TimescaleSchema;
import deltix.timebase.connector.service.MigrationService;
import deltix.timebase.messages.schema.*;
import deltix.util.collections.generated.ObjectArrayList;
import deltix.util.collections.generated.ObjectList;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Set;
import java.util.stream.Collectors;

@AllArgsConstructor
@Service
public class TimescaleMigrationService implements MigrationService<SchemaChangeMessage> {

    private static final Log LOG = LogFactory.getLog(TimescaleMigrationService.class);

    private final TimescaleSqlGenerator sqlGenerator;
    private final TimescaleDataService dataService;
    private final TimescaleSchemaDefinition schemaDefinition;
    private final TimescaleMigrationMetadataService migrationMetadataService;

    @Override
    @Transactional
    public void apply(SchemaChangeMessage model, String schemaName) {
        long migrationTimestamp = model.getTimeStampMs();
        long migrationVersion = model.getVersion();

        LOG.info().append("Try to handle migration message for scheme: ").append(schemaName).append(" with timestamp: ")
                .append(migrationTimestamp).commit();

        ObjectArrayList<ClassDescriptorInfo> newState = model.getNewState();
        TimescaleSchema newSchema = schemaDefinition.getTimebaseSchemaDefinition(newState, schemaName);
        ObjectArrayList<ClassDescriptorInfo> previousState = model.getPreviousState();
        TimescaleSchema previousSchema = schemaDefinition.getTimebaseSchemaDefinition(previousState, schemaName);

        ObjectArrayList<SchemaDescriptorChangeActionInfo> changes = model.getDescriptorChangeActions();
        changes.forEach(change -> {
            ClassDescriptorInfo previousDescriptor = change.getPreviousState();
            String previousDescriptorName = previousDescriptor != null ? previousDescriptor.getName().toString() : null;
            ClassDescriptorInfo newDescriptor = change.getNewState();
            String newDescriptorName = newDescriptor != null ? newDescriptor.getName().toString() : null;

            if (change.hasChangeTypes()) {
                //ALTER DELETE ADD etc.
                SchemaDescriptorChangeType changeType = change.getChangeTypes();

                switch (changeType) {
                    case ADD:
                        if (newDescriptor instanceof RecordClassDescriptor) {
                            Set<TimescaleColumn> addColumns = getTimescaleColumns(newSchema, (RecordClassDescriptorInfo) newDescriptor);
                            handleAddColumnsOperation(schemaName, addColumns);
                        }
                        break;
                    case DELETE:
                        if (previousDescriptor instanceof RecordClassDescriptor) {
                            Set<TimescaleColumn> removeColumns = getTimescaleColumns(previousSchema, (RecordClassDescriptor) previousDescriptor)
                                    .stream()
                                    .filter(column -> column.getRelatedDescriptors().size() <= 1)
                                    .collect(Collectors.toSet());
                            handleDropColumnsOperation(schemaName, removeColumns);
                        }
                        break;
                    case RENAME:
                        // rename descriptor_name field
                        if (previousDescriptor instanceof RecordClassDescriptor && newDescriptor instanceof RecordClassDescriptor) {
                            handleRenameDescriptorOperation(previousSchema, previousDescriptorName,
                                    newDescriptorName);
                        }
                        break;
                    case CONTENT_TYPE_CHANGE:
                        // remove all records with descriptor_name
                        handleDropRecordOperation(previousSchema, previousDescriptorName);
                        break;
                    case FIELDS_CHANGE:
                        // handle single column change
                        ObjectList<SchemaFieldChangeActionInfo> fieldChangeActions = change.getFieldChangeActions();
                        for (int i = 0; i < fieldChangeActions.size(); i++) {
                            SchemaFieldChangeActionInfo action = fieldChangeActions.getObject(i);
                            SchemaFieldChangeType fieldChangeType = action.getChangeTypes();
                            DataFieldInfo previousColumn = action.getPreviousState();
                            DataFieldInfo newColumn = action.getNewState();

                            switch (fieldChangeType) {
                                case RENAME:
                                    //rename field operation
                                    TimescaleColumn previousColumnForRename = getTimescaleColumn(previousSchema, previousColumn,
                                            previousDescriptorName);
                                    TimescaleColumn newColumnForRename = getTimescaleColumn(newSchema, newColumn, newDescriptorName);
                                    handleRenameColumnOperation(schemaName, previousColumnForRename, newColumnForRename);
                                    break;
                                case DELETE:
                                    // get previous state and delete this field
                                    TimescaleColumn previousColumnForDelete = getTimescaleColumn(previousSchema, previousColumn,
                                            previousDescriptorName);
                                    handleDropColumnOperation(schemaName, previousColumnForDelete);
                                    break;
                                case ADD:
                                    // get new state and add it
                                    TimescaleColumn newColumnForAdd = getTimescaleColumn(newSchema, newColumn, newDescriptorName);
                                    handleAddColumnOperation(schemaName, newColumnForAdd);
                                    break;
                                case DATA_TYPE_CHANGE:
                                    if (action.hasDataTransformation()) {
                                        SchemaFieldDataTransformationInfo dataTransformation = action.getDataTransformation();
                                        if (dataTransformation.hasTransformationType()) {
                                            SchemaFieldDataTransformationType transformationType = dataTransformation.getTransformationType();

                                            switch (transformationType) {
                                                case DROP_RECORD:
                                                    handleDropRecordOperation(previousSchema, previousDescriptorName);
                                                    break;
                                                case SET_DEFAULT:
                                                    // set new value where null
                                                    CharSequence defaultValue = dataTransformation.getDefaultValue();
                                                    TimescaleColumn newColumnWithDefaultValue = getTimescaleColumn(newSchema, newColumn, newDescriptorName);
                                                    handleSetDefaultValueOperation(schemaName, newColumnWithDefaultValue, defaultValue.toString());
                                                    break;
                                                case CONVERT_DATA:
                                                    TimescaleColumn newColumnWithConvertedType = getTimescaleColumn(newSchema, newColumn, newDescriptorName);
                                                    handleChangeDataTypeOperation(schemaName, newColumnWithConvertedType);
                                                    break;
                                                default:
                                                    LOG.warn().append("Unsupported transformation type: ").append(transformationType).commit();
                                                    break;
                                            }
                                        }
                                    }
                                    break;
                                case STATIC_VALUE_CHANGE:
                                    //rename all default prev default values to new one
                                    CharSequence defaultValue = action.getDataTransformation().getDefaultValue();
                                    TimescaleColumn newStaticColumn = getTimescaleColumn(newSchema, newColumn, newDescriptorName);
                                    handleChangeDefaultValueOperation(schemaName, newStaticColumn, defaultValue.toString());
                                    break;
                                case MODIFIER_CHANGE:
                                    TimescaleColumn columnWithChangedModifier = getTimescaleColumn(newSchema, newColumn, newDescriptorName);
                                    if (newColumn instanceof StaticDataField) {
                                        String newDefaultVal = action.getDataTransformation().getDefaultValue().toString();
                                        handleChangeDefaultValueOperation(schemaName, columnWithChangedModifier, newDefaultVal);
                                    } else {
                                        if (action.hasDataTransformation()) {
                                            if (action.getDataTransformation().hasDefaultValue()) {
                                                String newDefaultVal = action.getDataTransformation().getDefaultValue().toString();
                                                handleChangeDefaultValueOperation(schemaName, columnWithChangedModifier, newDefaultVal);
                                            }
                                        }
                                    }
                                    break;
                                case DESCRIPTION_CHANGE:
                                case PRIMARY_KEY_CHANGE:
                                case TITLE_CHANGE:
                                case ORDINAL_CHANGE:
                                case RELATION_CHANGE:
                                    LOG.debug().append("Ignore field change action: ").append(fieldChangeType).commit();
                                    break;
                                default:
                                    LOG.warn().append("Unsupported field change action: ").append(fieldChangeType).commit();
                                    break;
                            }
                        }
                        break;
                    default:
                        LOG.warn().append("There is no implementation for handling change type: ").append(changeType).commit();
                        break;
                }
            }
        });

        MigrationMetadata migrationMetadata = migrationMetadataService.getByStreamName(schemaName).get();
        migrationMetadata.setVersion(migrationVersion);
        migrationMetadata.setIsSuccess(Boolean.TRUE);
        migrationMetadata.setDateTime(System.currentTimeMillis());

        migrationMetadataService.update(migrationMetadata);
    }

    private void handleAddColumnsOperation(String schemaName, Set<TimescaleColumn> columns) {
        String addColumnsStatement = sqlGenerator.generateAddColumnStatement(schemaName, columns);
        dataService.executeQuery(addColumnsStatement);
    }

    private void handleAddColumnOperation(String schemaName, TimescaleColumn column) {
        String addColumnStatement = sqlGenerator.generateAddColumnStatement(schemaName, column);
        dataService.executeQuery(addColumnStatement);
    }

    private void handleDropColumnsOperation(String schemaName, Set<TimescaleColumn> columns) {
        String dropColumnsStatement = sqlGenerator.generateDropColumnStatement(schemaName, columns);
        dataService.executeQuery(dropColumnsStatement);
    }

    private void handleDropColumnOperation(String schemaName, TimescaleColumn column) {
        String dropColumnStatement = sqlGenerator.generateDropColumnStatement(schemaName, column);
        dataService.executeQuery(dropColumnStatement);
    }

    private void handleRenameDescriptorOperation(TimescaleSchema previousSchema, String previousDescriptorName, String newDescriptorName) {
        TimescaleColumn descriptorNameColumn = getTimescaleColumn(previousSchema, "descriptor_name", previousDescriptorName);
        String statement = sqlGenerator.generateRenameDescriptorNameColumnStatement(previousSchema.getName(),
                descriptorNameColumn.getName(), previousDescriptorName, newDescriptorName);
        dataService.executeQuery(statement);
    }

    private void handleDropRecordOperation(TimescaleSchema previousSchema, String previousDescriptorName) {
        TimescaleColumn descriptorNameColumn = getTimescaleColumn(previousSchema, "descriptor_name", previousDescriptorName);
        String statement = sqlGenerator.generateDropRecordStatement(previousSchema.getName(),
                descriptorNameColumn.getName(), previousDescriptorName);
        dataService.executeQuery(statement);
    }

    private void handleRenameColumnOperation(String schemaName, TimescaleColumn previousColumn, TimescaleColumn newColumn) {
        String renameColumnStatement = sqlGenerator.generateRenameColumnStatement(schemaName, previousColumn.getName(),
                newColumn.getName());
        dataService.executeQuery(renameColumnStatement);
    }

    private void handleChangeDataTypeOperation(String schemaName, TimescaleColumn column) {
        String changeDataTypeStatement = sqlGenerator.generateChangeDataTypeStatement(schemaName, column);
        dataService.executeQuery(changeDataTypeStatement);
    }

    private void handleSetDefaultValueOperation(String schemaName, TimescaleColumn column, String defaultValue) {
        String setDefaultValueStatement = sqlGenerator.generateSetDefaultValueStatement(schemaName, column, defaultValue);
        dataService.executeQuery(setDefaultValueStatement);
    }

    private void handleChangeDefaultValueOperation(String schemaName, TimescaleColumn column, String defaultValue) {
        String changeDefaultValueStatement = sqlGenerator.generateChangeDefaultValueStatement(schemaName, column, defaultValue);
        dataService.executeQuery(changeDefaultValueStatement);
    }

    private Set<TimescaleColumn> getTimescaleColumns(TimescaleSchema schema, RecordClassDescriptorInfo recordClassDescriptorInfo) {
        String descriptorName = recordClassDescriptorInfo.getName().toString();

        return schema.getColumns()
                .stream()
                .filter(column -> column.getRelatedDescriptors().contains(descriptorName))
                .collect(Collectors.toSet());
    }

    private TimescaleColumn getTimescaleColumn(TimescaleSchema schema, DataFieldInfo dataField, String descriptorName) {
        String fieldName = dataField.getName().toString();
        return schema.getColumns()
                .stream()
                .filter(column -> column.getRelatedDescriptors().contains(descriptorName))
                .filter(column -> {
                    String[] s = column.getName().split("\\.");
                    return s[s.length - 1].equals(fieldName);
                }).findAny()
                .orElseThrow(() -> new ColumnNotFoundException());
    }

    private TimescaleColumn getTimescaleColumn(TimescaleSchema schema, String fieldName, String descriptorName) {
        return schema.getColumns()
                .stream()
                .filter(column -> column.getRelatedDescriptors().contains(descriptorName))
                .filter(column -> {
                    String[] s = column.getName().split("\\.");
                    return s[s.length - 1].equals(fieldName);
                }).findAny()
                .orElseThrow(() -> new ColumnNotFoundException());
    }
}

package deltix.timebase.connector.service.timescale;

import deltix.gflog.Log;
import deltix.gflog.LogFactory;
import deltix.timebase.connector.model.schema.TimescaleColumn;
import deltix.timebase.connector.model.schema.TimescaleSchema;
import deltix.timebase.connector.util.ConnectorUtils;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Component
public class TimescaleSqlGenerator {

    private static final Log LOG = LogFactory.getLog(TimescaleSqlGenerator.class);

    static final String GET_MIGRATION_METADATA_STATEMENT = "SELECT * FROM migrations_tracker WHERE Stream = ?";
    static final String UPDATE_MIGRATION_METADATA_STATEMENT = "UPDATE migrations_tracker SET MigrationDateTime = :dateTime," +
            " IsSuccess = :isSuccess, Version = :version WHERE Id = :id";
    static final String INSERT_MIGRATION_METADATA_STATEMENT = "INSERT INTO migrations_tracker " +
            "(MigrationDateTime, IsSuccess, Version, Stream) " +
            "VALUES (:dateTime, :isSuccess, :version, :stream)";

    private static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE IF NOT EXISTS \"%s\" (%s)";
    private static final String CREATE_HYPERTABLE_TEMPLATE = "SELECT create_hypertable('\"%s\"', '%s', %s)";
    private static final String INSERT_QUERY_TEMPLATE = "INSERT INTO \"%s\" (%s) VALUES (%s)";
    private static final String PRIMARY_KEY_TEMPLATE = "PRIMARY KEY (%s)";

    private static final String ALTER_TABLE_STATEMENT = "ALTER TABLE \"%s\" %s";
    private static final String UPDATE_TABLE_STATEMENT = "UPDATE \"%s\" %s";
    private static final String ADD_COLUMN_STATEMENT = "ADD COLUMN IF NOT EXISTS %s %s";
    private static final String DROP_COLUMN_STATEMENT = "DROP COLUMN %s";
    private static final String RENAME_COLUMN_STATEMENT = "RENAME COLUMN %s TO %s";
    private static final String SET_DATA_TYPE_STATEMENT = "ALTER COLUMN %s SET DATA TYPE %s USING %s::%s";
    private static final String GET_LAST_TIMESTAMP_STATEMENT = "SELECT MAX(EventTime) AS EventTime FROM \"%s\"";
    private static final String DELETE_ROWS_WITH_TIMESTAMP_STATEMENT = "DELETE FROM \"%s\" WHERE EventTime = ?";
    private static final String CHANGE_CERTAIN_COLUMN_VALUE_STATEMENT = "SET %s = %s WHERE %s";
    private static final String CHANGE_ALL_COLUMN_VALUES_STATEMENT = "SET %s = %s";
    private static final String DELETE_RECORDS_WITH_CONDITION_STATEMENT = "DELETE FROM \"%s\" WHERE %s";

    public String generateCreateTableStatement(TimescaleSchema schema) {
        List<TimescaleColumn> columns = schema.getColumns();
        String name = schema.getName();
        TimescaleColumn[] primaryKey = schema.getPrimaryKey();

        String columnsDefinition = columns
                .stream()
                .map(column -> column.getName() + " " + ConnectorUtils.getSqlDataTypeName(column.getDataType()) + ",")
                .collect(Collectors.joining("\n"));

        String primaryKeyDefinition = String.format(
                PRIMARY_KEY_TEMPLATE,
                Arrays.asList(primaryKey)
                        .stream()
                        .map(TimescaleColumn::getName)
                        .collect(Collectors.joining(", ")));

        return String.format(CREATE_TABLE_TEMPLATE, name, columnsDefinition + "\n" + primaryKeyDefinition);
    }

    public String generateHypertableStatement(TimescaleSchema schema) {
        String name = schema.getName();
        return String.format(CREATE_HYPERTABLE_TEMPLATE, name, "eventtime", "chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE");
    }

    public String generateInsertStatement(TimescaleSchema schema) {
        String name = schema.getName();
        List<TimescaleColumn> columns = schema.getColumns();

        String columnNames = columns.stream()
                .filter(column -> !"Id".equals(column.getName()))
                .map(TimescaleColumn::getName)
                .collect(Collectors.joining(","));

        String values = columns.stream()
                .filter(column -> !"Id".equals(column.getName()))
                .map(column -> "?")
                .collect(Collectors.joining(","));


        return String.format(INSERT_QUERY_TEMPLATE, name, columnNames, values);
    }

    public String generateAddColumnStatement(String schema, TimescaleColumn column) {
        String addColumnStatement = String.format(ADD_COLUMN_STATEMENT, column.getName(), ConnectorUtils.getSqlDataTypeName(column.getDataType()));

        return String.format(ALTER_TABLE_STATEMENT, schema, addColumnStatement);
    }

    public String generateAddColumnStatement(String schema, Set<TimescaleColumn> columns) {
        String addColumnsStatement = columns.stream()
                .map(column ->  String.format(ADD_COLUMN_STATEMENT, column.getName(), ConnectorUtils.getSqlDataTypeName(column.getDataType())))
                .collect(Collectors.joining(", "));

        return String.format(ALTER_TABLE_STATEMENT, schema, addColumnsStatement);
    }

    public String generateDropColumnStatement(String schema, TimescaleColumn column) {
        String dropColumnStatement = String.format(DROP_COLUMN_STATEMENT, column.getName());

        return String.format(ALTER_TABLE_STATEMENT, schema, dropColumnStatement);
    }

    public String generateDropColumnStatement(String schema, Set<TimescaleColumn> columns) {
        String dropColumnsStatement = columns.stream()
                .map(column ->  String.format(DROP_COLUMN_STATEMENT, column.getName()))
                .collect(Collectors.joining(", "));

        return String.format(ALTER_TABLE_STATEMENT, schema, dropColumnsStatement);
    }

    public String generateChangeDataTypeStatement(String schema, TimescaleColumn column) {
        String setDataTypeStatement = String.format(SET_DATA_TYPE_STATEMENT, column.getName(), ConnectorUtils.getSqlDataTypeName(column.getDataType()), column.getName(), ConnectorUtils.getSqlDataTypeName(column.getDataType()));

        return String.format(ALTER_TABLE_STATEMENT, schema, setDataTypeStatement);
    }

    // tested but not used
    public String generateChangeDataTypeStatement(String schema, Set<TimescaleColumn> columns) {
        String setDataTypeColumnsStatement = columns.stream()
                .map(column ->  String.format(SET_DATA_TYPE_STATEMENT, column.getName(), ConnectorUtils.getSqlDataTypeName(column.getDataType()), column.getName(), ConnectorUtils.getSqlDataTypeName(column.getDataType())))
                .collect(Collectors.joining(", "));

        return String.format(ALTER_TABLE_STATEMENT, schema, setDataTypeColumnsStatement);
    }

    public String generateRenameColumnStatement(String schema, String oldName, String newName) {
        String renameStatement = String.format(RENAME_COLUMN_STATEMENT, oldName, newName);

        return String.format(ALTER_TABLE_STATEMENT, schema, renameStatement);
    }

    public String generateLastTimestampStatement(String schema) {
        return String.format(GET_LAST_TIMESTAMP_STATEMENT, schema);
    }

    public String generateDeleteLastTimestampStatement(String schema) {
        return String.format(DELETE_ROWS_WITH_TIMESTAMP_STATEMENT, schema);
    }

    public String generateSetDefaultValueStatement(String schema, TimescaleColumn column, String defaultValue) {
        String setDefaultValueStatement = String.format(CHANGE_CERTAIN_COLUMN_VALUE_STATEMENT, column.getName(), defaultValue, column.getName() + " = NULL");
        return String.format(UPDATE_TABLE_STATEMENT, schema, setDefaultValueStatement);
    }

    public String generateChangeDefaultValueStatement(String schema, TimescaleColumn column, String newDefaultValue) {
        String changeDefaultValueStatement = String.format(CHANGE_ALL_COLUMN_VALUES_STATEMENT, column.getName(), newDefaultValue);
        return String.format(UPDATE_TABLE_STATEMENT, schema, changeDefaultValueStatement);
    }

    public String generateRenameDescriptorNameColumnStatement(String schema, String columnName, String previousValue, String newValue) {
        String renameDescriptorNameColumnStatement = String.format(CHANGE_CERTAIN_COLUMN_VALUE_STATEMENT, columnName, "'" + newValue + "'", columnName + " = " + "'" + previousValue + "'");

        return String.format(UPDATE_TABLE_STATEMENT, schema, renameDescriptorNameColumnStatement);
    }

    public String generateDropRecordStatement(String schema, String columnDescriptorName, String valueOfCondition) {
        String deleteStatement = columnDescriptorName + " = " + "'" + valueOfCondition + "'";

        return String.format(DELETE_RECORDS_WITH_CONDITION_STATEMENT, schema, deleteStatement);
    }
}

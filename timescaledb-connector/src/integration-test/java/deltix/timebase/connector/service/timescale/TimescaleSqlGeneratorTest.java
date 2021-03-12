package deltix.timebase.connector.service.timescale;

import deltix.timebase.connector.model.schema.TimescaleColumn;
import deltix.timebase.connector.model.schema.TimescaleSchema;
import deltix.timebase.connector.service.BaseServiceIntegrationTest;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.BadSqlGrammarException;

import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TimescaleSqlGeneratorTest extends BaseServiceIntegrationTest {

    @Autowired
    private TimescaleSqlGenerator sqlGenerator;

    @Autowired
    private TimescaleDataService dataService;

    @Test
    public void testTimescaleSchemaGeneration() {
        TimescaleSchema schema = getValidSchema();

        String schemaStatement = sqlGenerator.generateCreateTableStatement(schema);

        dataService.executeQuery(schemaStatement);
        dataService.executeQuery("select * from \"orders-123\"");
    }

    @Test
    public void testTimescaleSchemaGenerationFailed() {
        TimescaleSchema schema = getInvalidSchema();

        String schemaStatement = sqlGenerator.generateCreateTableStatement(schema);

        assertThrows(BadSqlGrammarException.class, () -> dataService.executeQuery(schemaStatement));
    }

    @Test
    public void testTimescaleHypertableGeneration() {
        TimescaleSchema schema = getValidSchema();

        String createTableStatement = sqlGenerator.generateCreateTableStatement(schema);

        dataService.executeQuery(createTableStatement);

        String createHypertableStatement = sqlGenerator.generateHypertableStatement(schema);

        dataService.executeQuery(createHypertableStatement);

        assertThat(createHypertableStatement, is("SELECT create_hypertable('\"orders-123\"', 'eventtime', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE)"));
    }

    @Test
    public void testTimescaleHypertableGenerationFailed() {
        TimescaleSchema schema = getValidSchema();

        String createHypertableStatement = sqlGenerator.generateHypertableStatement(schema);

        assertThrows(BadSqlGrammarException.class, () -> dataService.executeQuery(createHypertableStatement));
    }

    @Test
    public void testTimescaleAddColumnGeneration() {
        String schemaName = "orders-123";
        TimescaleSchema schema = getValidSchema();

        TimescaleColumn newColumn = TimescaleColumn.builder()
                .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                .name("newColumn")
                .build();

        String createTableStatement = sqlGenerator.generateCreateTableStatement(schema);
        String createHypertableStatement = sqlGenerator.generateHypertableStatement(schema);
        String addColumnStatement = sqlGenerator.generateAddColumnStatement(schemaName, newColumn);

        dataService.executeQuery(createTableStatement);
        dataService.executeQuery(createHypertableStatement);
        dataService.executeQuery(addColumnStatement);

        assertThat(addColumnStatement, is("ALTER TABLE \"orders-123\" ADD COLUMN IF NOT EXISTS newColumn VARCHAR"));
    }

    @Test
    public void testTimescaleAddColumnsGeneration() {
        String schemaName = "orders-123";
        TimescaleSchema schema = getValidSchema();

        TimescaleColumn newColumn1 = TimescaleColumn.builder()
                .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                .name("newColumn1")
                .build();
        TimescaleColumn newColumn2 = TimescaleColumn.builder()
                .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                .name("newColumn2")
                .build();
        Set<TimescaleColumn> columns = new HashSet<>();
        columns.add(newColumn1);
        columns.add(newColumn2);

        String createTableStatement = sqlGenerator.generateCreateTableStatement(schema);
        String createHypertableStatement = sqlGenerator.generateHypertableStatement(schema);
        String addColumnsStatement = sqlGenerator.generateAddColumnStatement(schemaName, columns);

        dataService.executeQuery(createTableStatement);
        dataService.executeQuery(createHypertableStatement);
        dataService.executeQuery(addColumnsStatement);

        assertThat(addColumnsStatement, is(Matchers.oneOf(
                "ALTER TABLE \"orders-123\" ADD COLUMN IF NOT EXISTS newColumn2 VARCHAR, ADD COLUMN IF NOT EXISTS newColumn1 VARCHAR",
                "ALTER TABLE \"orders-123\" ADD COLUMN IF NOT EXISTS newColumn1 VARCHAR, ADD COLUMN IF NOT EXISTS newColumn2 VARCHAR")));
    }

    @Test
    public void testTimescaleDropColumnGeneration() {
        String schemaName = "orders-123";
        TimescaleSchema schema = getValidSchema();

        TimescaleColumn dropColumn = TimescaleColumn.builder()
                .dataType(TimescaleColumn.TimescaleDataType.DECIMAL64)
                .name("balance")
                .build();

        String createTableStatement = sqlGenerator.generateCreateTableStatement(schema);
        String createHypertableStatement = sqlGenerator.generateHypertableStatement(schema);
        String dropColumnStatement = sqlGenerator.generateDropColumnStatement(schemaName, dropColumn);

        dataService.executeQuery(createTableStatement);
        dataService.executeQuery(createHypertableStatement);
        dataService.executeQuery(dropColumnStatement);

        assertThat(dropColumnStatement, is("ALTER TABLE \"orders-123\" DROP COLUMN balance"));
    }

    @Test
    public void testTimescaleDropColumnsGeneration() {
        String schemaName = "orders-123";
        TimescaleSchema schema = getValidSchema();

        TimescaleColumn dropColumn1 = TimescaleColumn.builder()
                .dataType(TimescaleColumn.TimescaleDataType.DECIMAL64)
                .name("balance")
                .build();

        TimescaleColumn dropColumn2 = TimescaleColumn.builder()
                .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                .name("somename")
                .build();
        Set<TimescaleColumn> columns = new HashSet<>();
        columns.add(dropColumn1);
        columns.add(dropColumn2);

        String createTableStatement = sqlGenerator.generateCreateTableStatement(schema);
        String createHypertableStatement = sqlGenerator.generateHypertableStatement(schema);
        String dropColumnStatement = sqlGenerator.generateDropColumnStatement(schemaName, columns);

        dataService.executeQuery(createTableStatement);
        dataService.executeQuery(createHypertableStatement);
        dataService.executeQuery(dropColumnStatement);

        assertThat(dropColumnStatement, is(Matchers.oneOf(
                "ALTER TABLE \"orders-123\" DROP COLUMN somename, DROP COLUMN balance",
                "ALTER TABLE \"orders-123\" DROP COLUMN balance, DROP COLUMN somename")));
    }

    @Test
    public void testTimescaleSetDataTypeGeneration() {
        String schemaName = "orders-123";
        TimescaleSchema schema = getValidSchema();

        TimescaleColumn newColumn = TimescaleColumn.builder()
                .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                .name("balance")
                .build();

        String createTableStatement = sqlGenerator.generateCreateTableStatement(schema);
        String createHypertableStatement = sqlGenerator.generateHypertableStatement(schema);
        String setColumnDataTypeStatement = sqlGenerator.generateChangeDataTypeStatement(schemaName, newColumn);

        dataService.executeQuery(createTableStatement);
        dataService.executeQuery(createHypertableStatement);
        dataService.executeQuery(setColumnDataTypeStatement);

        assertThat(setColumnDataTypeStatement, is("ALTER TABLE \"orders-123\" ALTER COLUMN balance SET DATA TYPE VARCHAR" +
                " USING balance::VARCHAR"));
    }

    @Test
    public void testTimescaleSetDataTypesGeneration() {
        String schemaName = "orders-123";
        TimescaleSchema schema = getValidSchema();

        TimescaleColumn newColumn1 = TimescaleColumn.builder()
                .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                .name("balance")
                .build();
        TimescaleColumn newColumn2 = TimescaleColumn.builder()
                .dataType(TimescaleColumn.TimescaleDataType.INTEGER)
                .name("somename")
                .build();
        Set<TimescaleColumn> columns = new HashSet<>();
        columns.add(newColumn1);
        columns.add(newColumn2);

        String createTableStatement = sqlGenerator.generateCreateTableStatement(schema);
        String createHypertableStatement = sqlGenerator.generateHypertableStatement(schema);
        String setColumnDataTypeStatement = sqlGenerator.generateChangeDataTypeStatement(schemaName, columns);

        dataService.executeQuery(createTableStatement);
        dataService.executeQuery(createHypertableStatement);
        dataService.executeQuery(setColumnDataTypeStatement);

        assertThat(setColumnDataTypeStatement, is(Matchers.oneOf(
                "ALTER TABLE \"orders-123\" ALTER COLUMN balance SET DATA TYPE VARCHAR USING balance::VARCHAR, ALTER COLUMN somename SET DATA TYPE INTEGER USING somename::INTEGER",
                "ALTER TABLE \"orders-123\" ALTER COLUMN somename SET DATA TYPE INTEGER USING somename::INTEGER, ALTER COLUMN balance SET DATA TYPE VARCHAR USING balance::VARCHAR")));
    }

    @Test
    public void testTimescaleLastTimestampGeneration() {
        String schemaName = "orders-123";
        TimescaleSchema schema = getValidSchema();

        String createTableStatement = sqlGenerator.generateCreateTableStatement(schema);
        String createHypertableStatement = sqlGenerator.generateHypertableStatement(schema);
        String lastTimestampStatement = sqlGenerator.generateLastTimestampStatement(schemaName);

        dataService.executeQuery(createTableStatement);
        dataService.executeQuery(createHypertableStatement);
        long lastTimestamp = dataService.getLastTimestamp(lastTimestampStatement);

        assertThat(lastTimestamp, is(0l));
        assertThat(lastTimestampStatement, is("SELECT MAX(EventTime) AS EventTime FROM \"orders-123\""));
    }

    @Test
    public void testDeleteLastTimestampGeneration() {
        String schemaName = "orders-123";

        String deleteLastTimestampStatement = sqlGenerator.generateDeleteLastTimestampStatement(schemaName);

        assertThat(deleteLastTimestampStatement, is("DELETE FROM \"orders-123\" WHERE EventTime = ?"));
    }

    @Test
    public void testSetDefaultValueGeneration() {
        String schemaName = "orders-123";
        TimescaleSchema schema = getValidSchema();

        TimescaleColumn balance = TimescaleColumn.builder()
                .name("balance")
                .dataType(TimescaleColumn.TimescaleDataType.DECIMAL64)
                .build();

        String createTableStatement = sqlGenerator.generateCreateTableStatement(schema);
        String createHypertableStatement = sqlGenerator.generateHypertableStatement(schema);
        String setDefaultValueStatement = sqlGenerator.generateSetDefaultValueStatement(schemaName, balance, "127.12558");

        dataService.executeQuery(createTableStatement);
        dataService.executeQuery(createHypertableStatement);
        dataService.executeQuery(setDefaultValueStatement);

        assertThat(setDefaultValueStatement, is("UPDATE \"orders-123\" SET balance = 127.12558 WHERE balance = NULL"));
    }

    @Test
    public void testChangeDefaultValueGeneration() {
        String schemaName = "orders-123";
        TimescaleSchema schema = getValidSchema();

        TimescaleColumn balance = TimescaleColumn.builder()
                .name("balance")
                .dataType(TimescaleColumn.TimescaleDataType.DECIMAL64)
                .build();

        String createTableStatement = sqlGenerator.generateCreateTableStatement(schema);
        String createHypertableStatement = sqlGenerator.generateHypertableStatement(schema);
        String setDefaultValueStatement = sqlGenerator.generateChangeDefaultValueStatement(schemaName, balance, "127.12558");

        dataService.executeQuery(createTableStatement);
        dataService.executeQuery(createHypertableStatement);
        dataService.executeQuery(setDefaultValueStatement);

        assertThat(setDefaultValueStatement, is("UPDATE \"orders-123\" SET balance = 127.12558"));
    }

    @Test
    public void testGenerateRenameColumnStatement() {
        TimescaleSchema schema = getValidSchema();

        String oldColumnName = "balance";
        String newColumnName = "newBalance";

        String createTableStatement = sqlGenerator.generateCreateTableStatement(schema);
        String createHypertableStatement = sqlGenerator.generateHypertableStatement(schema);
        String renameColumnStatement = sqlGenerator.generateRenameColumnStatement(schema.getName(), oldColumnName, newColumnName);

        dataService.executeQuery(createTableStatement);
        dataService.executeQuery(createHypertableStatement);
        dataService.executeQuery(renameColumnStatement);

        assertThat(renameColumnStatement, is("ALTER TABLE \"orders-123\" RENAME COLUMN balance TO newBalance"));
    }

    @Test
    public void testGenerateRenameDescriptorNameColumnStatement() {
        TimescaleSchema schema = getValidSchema();

        TimescaleColumn descriptorNameColumn = TimescaleColumn.builder()
                .name("descriptor_name")
                .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                .relatedDescriptors(Collections.emptyList())
                .build();

        List<TimescaleColumn> columns = new ArrayList<>();
        columns.add(descriptorNameColumn);
        columns.addAll(schema.getColumns());

        schema.setColumns(columns);

        String createTableStatement = sqlGenerator.generateCreateTableStatement(schema);
        String createHypertableStatement = sqlGenerator.generateHypertableStatement(schema);
        String renameDescriptorNameColumnStatement = sqlGenerator.generateRenameDescriptorNameColumnStatement(schema.getName(),
                descriptorNameColumn.getName(), "prevVal", "newVal");

        dataService.executeQuery(createTableStatement);
        dataService.executeQuery(createHypertableStatement);
        dataService.executeQuery(renameDescriptorNameColumnStatement);

        assertThat(renameDescriptorNameColumnStatement, is("UPDATE \"orders-123\" SET descriptor_name = 'newVal' WHERE descriptor_name = 'prevVal'"));
    }

    @Test
    public void testGenerateDropRecordStatement() {
        TimescaleSchema schema = getValidSchema();

        TimescaleColumn descriptorNameColumn = TimescaleColumn.builder()
                .name("descriptor_name")
                .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                .relatedDescriptors(Collections.emptyList())
                .build();

        List<TimescaleColumn> columns = new ArrayList<>();
        columns.add(descriptorNameColumn);
        columns.addAll(schema.getColumns());

        schema.setColumns(columns);

        String createTableStatement = sqlGenerator.generateCreateTableStatement(schema);
        String createHypertableStatement = sqlGenerator.generateHypertableStatement(schema);
        String dropRecordStatement = sqlGenerator.generateDropRecordStatement(schema.getName(),
                descriptorNameColumn.getName(), "prevDescriptorName");

        dataService.executeQuery(createTableStatement);
        dataService.executeQuery(createHypertableStatement);
        dataService.executeQuery(dropRecordStatement);

        assertThat(dropRecordStatement, is("DELETE FROM \"orders-123\" WHERE descriptor_name = 'prevDescriptorName'"));
    }

    private TimescaleSchema getValidSchema() {
        TimescaleSchema schema = new TimescaleSchema();
        schema.setName("orders-123");
        schema.setPolymorphic(true);

        TimescaleColumn id = TimescaleColumn.builder()
                .name("id")
                .dataType(TimescaleColumn.TimescaleDataType.SERIAL)
                .build();

        TimescaleColumn eventtime = TimescaleColumn.builder()
                .name("EventTime")
                .dataType(TimescaleColumn.TimescaleDataType.DATETIME)
                .build();

        TimescaleColumn balance = TimescaleColumn.builder()
                .name("balance")
                .dataType(TimescaleColumn.TimescaleDataType.DECIMAL64)
                .build();

        TimescaleColumn somename = TimescaleColumn.builder()
                .name("somename")
                .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                .build();

        TimescaleColumn isSomething = TimescaleColumn.builder()
                .name("isSomething")
                .dataType(TimescaleColumn.TimescaleDataType.BOOLEAN)
                .build();

        TimescaleColumn val = TimescaleColumn.builder()
                .name("val")
                .dataType(TimescaleColumn.TimescaleDataType.INTEGER)
                .build();

        List<TimescaleColumn> columns = Arrays.asList(id, eventtime, balance, somename, isSomething, val);

        schema.setColumns(columns);
        schema.setPrimaryKey(new TimescaleColumn[]{id, eventtime});

        return schema;
    }

    private TimescaleSchema getInvalidSchema() {
        TimescaleSchema schema = new TimescaleSchema();
        schema.setName("orders-123");
        schema.setPolymorphic(true);

        TimescaleColumn id = TimescaleColumn.builder()
                .name("id")
                .dataType(TimescaleColumn.TimescaleDataType.SERIAL)
                .build();

        TimescaleColumn eventtime = TimescaleColumn.builder()
                .name("eventtime")
                .dataType(TimescaleColumn.TimescaleDataType.DATETIME)
                .build();

        TimescaleColumn balance = TimescaleColumn.builder()
                .name("balance")
                .dataType(TimescaleColumn.TimescaleDataType.DECIMAL64)
                .build();

        TimescaleColumn somename = TimescaleColumn.builder()
                .name("somename")
                .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                .build();

        TimescaleColumn isSomething = TimescaleColumn.builder()
                .name("isSomething")
                .dataType(TimescaleColumn.TimescaleDataType.BOOLEAN)
                .build();

        TimescaleColumn val = TimescaleColumn.builder()
                .name("val")
                .dataType(TimescaleColumn.TimescaleDataType.INTEGER)
                .build();

        List<TimescaleColumn> columns = Arrays.asList(id, eventtime, balance, somename, isSomething, val);

        schema.setColumns(columns);
        schema.setPrimaryKey(new TimescaleColumn[]{});

        return schema;
    }
}

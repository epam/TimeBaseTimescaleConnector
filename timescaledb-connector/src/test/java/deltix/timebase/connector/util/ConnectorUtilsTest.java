package deltix.timebase.connector.util;

import deltix.qsrv.hf.pub.md.*;
import deltix.timebase.connector.model.schema.TimescaleColumn;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class ConnectorUtilsTest {

    @Test
    public void testConvert() {
        DataField stringDataField = new NonStaticDataField("name", "title", VarcharDataType.getDefaultInstance());
        TimescaleColumn expectedVarcharColumn = TimescaleColumn.builder()
                .name("owner_name")
                .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                .relatedDescriptors(Collections.singletonList("DescriptorName"))
                .build();
        TimescaleColumn actualVarcharColumn = ConnectorUtils.convert(stringDataField, "owner", "DescriptorName");

        DataField dateTimeDataField = new NonStaticDataField("name", "title", DateTimeDataType.getDefaultInstance());
        TimescaleColumn expectedDateTimeColumn = TimescaleColumn.builder()
                .name("name")
                .dataType(TimescaleColumn.TimescaleDataType.DATETIME)
                .relatedDescriptors(Collections.emptyList())
                .build();
        TimescaleColumn actualDateTimeColumn = ConnectorUtils.convert(dateTimeDataField, null, null);

        DataField integerDataField = new NonStaticDataField("name", "title", IntegerDataType.getDefaultInstance());
        TimescaleColumn expectedIntegerColumn = TimescaleColumn.builder()
                .name("owner_parent_name")
                .dataType(TimescaleColumn.TimescaleDataType.LONG)
                .relatedDescriptors(Collections.emptyList())
                .build();
        TimescaleColumn actualIntegerColumn = ConnectorUtils.convert(integerDataField, "owner_parent", null);

        DataField booleanDataField = new NonStaticDataField("name", "title", BooleanDataType.getDefaultInstance());
        TimescaleColumn expectedBooleanColumn = TimescaleColumn.builder()
                .name("name")
                .dataType(TimescaleColumn.TimescaleDataType.BOOLEAN)
                .relatedDescriptors(Collections.singletonList("DescriptorName"))
                .build();
        TimescaleColumn actualBooleanColumn = ConnectorUtils.convert(booleanDataField, null, "DescriptorName");

        DataField decimalDataField = new NonStaticDataField("name", "title", FloatDataType.getDefaultInstance());
        TimescaleColumn expectedDecimalColumn = TimescaleColumn.builder()
                .name("name")
                .dataType(TimescaleColumn.TimescaleDataType.DECIMAL)
                .relatedDescriptors(Collections.emptyList())
                .build();
        TimescaleColumn actualDecimalColumn = ConnectorUtils.convert(decimalDataField, null, null);

        DataField byteArrayDataField = new NonStaticDataField("name", "title", BinaryDataType.getDefaultInstance());
        TimescaleColumn expectedByteArrayColumn = TimescaleColumn.builder()
                .name("name")
                .dataType(TimescaleColumn.TimescaleDataType.BYTEA)
                .relatedDescriptors(Collections.emptyList())
                .build();
        TimescaleColumn actualByteArrayColumn = ConnectorUtils.convert(byteArrayDataField, null, null);

        assertThat("Could not convert Timebase VarcharDataField properly", expectedVarcharColumn, is(actualVarcharColumn));
        assertThat("Could not convert Timebase DateTimeDataField properly", expectedDateTimeColumn, is(actualDateTimeColumn));
        assertThat("Could not convert Timebase IntegerDataField properly", expectedIntegerColumn, is(actualIntegerColumn));
        assertThat("Could not convert Timebase BooleanDataField properly", expectedBooleanColumn, is(actualBooleanColumn));
        assertThat("Could not convert Timebase DecimalDataField properly", expectedDecimalColumn, is(actualDecimalColumn));
        assertThat("Could not convert Timebase BinaryDataField properly", expectedByteArrayColumn, is(actualByteArrayColumn));
    }

    @Test
    public void testSqlTypeName() {
        assertThat(ConnectorUtils.getSqlDataTypeName(TimescaleColumn.TimescaleDataType.TIME), is("TIME"));
        assertThat(ConnectorUtils.getSqlDataTypeName(TimescaleColumn.TimescaleDataType.LONG), is("BIGINT"));
        assertThat(ConnectorUtils.getSqlDataTypeName(TimescaleColumn.TimescaleDataType.JSON), is("JSON"));
        assertThat(ConnectorUtils.getSqlDataTypeName(TimescaleColumn.TimescaleDataType.JSONB), is("JSONB"));
        assertThat(ConnectorUtils.getSqlDataTypeName(TimescaleColumn.TimescaleDataType.BOOLEAN), is("BOOLEAN"));
        assertThat(ConnectorUtils.getSqlDataTypeName(TimescaleColumn.TimescaleDataType.BYTEA), is("BYTEA"));
        assertThat(ConnectorUtils.getSqlDataTypeName(TimescaleColumn.TimescaleDataType.CHAR), is("CHAR"));
        assertThat(ConnectorUtils.getSqlDataTypeName(TimescaleColumn.TimescaleDataType.DATE), is("DATE"));
        assertThat(ConnectorUtils.getSqlDataTypeName(TimescaleColumn.TimescaleDataType.DATETIME), is("TIMESTAMP"));
        assertThat(ConnectorUtils.getSqlDataTypeName(TimescaleColumn.TimescaleDataType.DECIMAL), is("DECIMAL"));
        assertThat(ConnectorUtils.getSqlDataTypeName(TimescaleColumn.TimescaleDataType.DECIMAL64), is("DECIMAL(36, 18)"));
        assertThat(ConnectorUtils.getSqlDataTypeName(TimescaleColumn.TimescaleDataType.INTEGER), is("INTEGER"));
        assertThat(ConnectorUtils.getSqlDataTypeName(TimescaleColumn.TimescaleDataType.VARCHAR), is("VARCHAR"));
        assertThat(ConnectorUtils.getSqlDataTypeName(TimescaleColumn.TimescaleDataType.UUID), is("UUID"));
        assertThat(ConnectorUtils.getSqlDataTypeName(TimescaleColumn.TimescaleDataType.SERIAL), is("SERIAL"));
    }

    @Test
    public void testShortDescriptorName() {
        assertThat(ConnectorUtils.getShortDescriptorName("org.deltix.message.SomeClassName"), is("SomeClassName"));
        assertThat(ConnectorUtils.getShortDescriptorName("SomeClassName"), is("SomeClassName"));
    }
}

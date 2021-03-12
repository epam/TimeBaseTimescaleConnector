package deltix.timebase.connector.service.timescale;

import deltix.gflog.Log;
import deltix.gflog.LogFactory;
import deltix.qsrv.hf.pub.md.ClassDescriptor;
import deltix.qsrv.hf.pub.md.RecordClassDescriptor;
import deltix.qsrv.hf.tickdb.pub.DXTickStream;
import deltix.timebase.connector.model.schema.TimescaleColumn;
import deltix.timebase.connector.model.schema.TimescaleSchema;
import deltix.timebase.connector.service.timebase.TimebaseDescriptorService;
import deltix.timebase.connector.util.RecordClassDescriptorUtils;
import deltix.timebase.messages.schema.ClassDescriptorInfo;
import deltix.util.collections.generated.ObjectArrayList;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

@Service
@AllArgsConstructor
public class TimescaleSchemaDefinition {

    private static final Log LOG = LogFactory.getLog(TimescaleSchemaDefinition.class);

    private final TimebaseDescriptorService descriptorService;
    //private final JdbcTemplate jdbcTemplate;

    public TimescaleSchema getTimebaseSchemaDefinition(ClassDescriptor[] descriptors, String streamName) {
        TimescaleSchema schema = new TimescaleSchema();
        schema.setName(streamName);

        List<RecordClassDescriptor> baseClassDescriptors = RecordClassDescriptorUtils.getBaseClassDescriptors(descriptors);
        schema.setColumns(descriptorService.getColumns(baseClassDescriptors));
        schema.setPolymorphic(baseClassDescriptors.size() > 1 ? Boolean.TRUE : Boolean.FALSE);

        //add Id, EventTime, symbol, instrumentType
        TimescaleColumn id = TimescaleColumn.builder()
                .dataType(TimescaleColumn.TimescaleDataType.SERIAL)
                .relatedDescriptors(Collections.emptyList())
                .name("Id")
                .build();
        schema.getColumns().add(id);

        TimescaleColumn eventTime = TimescaleColumn.builder()
                .dataType(TimescaleColumn.TimescaleDataType.DATETIME)
                .relatedDescriptors(Collections.emptyList())
                .name("EventTime")
                .build();
        schema.getColumns().add(eventTime);

        TimescaleColumn symbol = TimescaleColumn.builder()
                .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                .relatedDescriptors(Collections.emptyList())
                .name("Symbol")
                .build();
        schema.getColumns().add(symbol);
        //end

        schema.setPrimaryKey(new TimescaleColumn[]{id, eventTime});

        return schema;
    }

    public TimescaleSchema getTimebaseSchemaDefinition(DXTickStream stream) {
        String streamName = stream.getName();
        ClassDescriptor[] allDescriptors = stream.getAllDescriptors();

        return getTimebaseSchemaDefinition(allDescriptors, streamName);
    }

    public TimescaleSchema getTimebaseSchemaDefinition(ObjectArrayList<ClassDescriptorInfo> classDescriptors, String streamName) {
        TimescaleSchema schema = new TimescaleSchema();
        schema.setName(streamName);

        schema.setColumns(descriptorService.getColumnsFromMessages(classDescriptors));

        //add Id, EventTime, symbol, instrumentType
        TimescaleColumn id = TimescaleColumn.builder()
                .dataType(TimescaleColumn.TimescaleDataType.SERIAL)
                .relatedDescriptors(Collections.emptyList())
                .name("Id")
                .build();
        schema.getColumns().add(id);

        TimescaleColumn eventTime = TimescaleColumn.builder()
                .dataType(TimescaleColumn.TimescaleDataType.DATETIME)
                .relatedDescriptors(Collections.emptyList())
                .name("EventTime")
                .build();
        schema.getColumns().add(eventTime);

        TimescaleColumn symbol = TimescaleColumn.builder()
                .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                .relatedDescriptors(Collections.emptyList())
                .name("Symbol")
                .build();
        schema.getColumns().add(symbol);

        schema.setPrimaryKey(new TimescaleColumn[]{id, eventTime});

        return schema;
    }

    //TODO implement restoring TimescaleSchema from Timescale
/*    public TimescaleSchema getTimescaleSchemaDefinition(String tableName) {
        TimescaleSchema schema = new TimescaleSchema();
        schema.setName(tableName);
        Set<TimescaleColumn> columns = new HashSet<>();
        try {
            Connection connection = jdbcTemplate.getDataSource().getConnection();
            ResultSet resultSet = connection.createStatement().executeQuery(String.format("select * from %s limit 1", tableName));
            ResultSetMetaData rsMetaData = resultSet.getMetaData();
            int columnCount = rsMetaData.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = rsMetaData.getColumnName(i);
                int columnType = rsMetaData.getColumnType(i);
                String typeName = JDBCType.valueOf(columnType).getName();
                int precision = rsMetaData.getPrecision(i);
                int scale = rsMetaData.getScale(i);
                String columnClassName = rsMetaData.getColumnClassName(i);

                System.out.println("Column: " + columnName + " type: " + typeName + " precision: " + precision + " scale: " + scale + " columnClassName: " + columnClassName);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return schema;
    }*/
}

package deltix.timebase.connector.service.timescale;

import deltix.gflog.Log;
import deltix.gflog.LogFactory;
import deltix.timebase.connector.model.MigrationMetadata;
import deltix.timebase.connector.model.schema.TimescaleColumn;
import deltix.timebase.connector.model.schema.TimescaleSchema;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

@Service
@AllArgsConstructor
public class TimescaleMigrationMetadataService {

    private static final Log LOG = LogFactory.getLog(TimescaleMigrationMetadataService.class);

    private final TimescaleSqlGenerator sqlGenerator;
    private final TimescaleDataService dataService;

    public void createMigrationTable() {
        TimescaleSchema migrationTable = TimescaleSchema.builder()
                .name("migrations_tracker")
                .columns(Arrays.asList(
                        TimescaleColumn.builder()
                                .name("Id")
                                .dataType(TimescaleColumn.TimescaleDataType.SERIAL)
                                .relatedDescriptors(Collections.emptyList())
                                .build(),
                        TimescaleColumn.builder()
                                .name("Stream")
                                .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                                .relatedDescriptors(Collections.emptyList())
                                .build(),
                        TimescaleColumn.builder()
                                .name("Version")
                                .dataType(TimescaleColumn.TimescaleDataType.LONG)
                                .relatedDescriptors(Collections.emptyList())
                                .build(),
                        TimescaleColumn.builder()
                                .name("IsSuccess")
                                .dataType(TimescaleColumn.TimescaleDataType.BOOLEAN)
                                .relatedDescriptors(Collections.emptyList())
                                .build(),
                        TimescaleColumn.builder()
                                .name("MigrationDateTime")
                                .dataType(TimescaleColumn.TimescaleDataType.DATETIME)
                                .relatedDescriptors(Collections.emptyList())
                                .build()
                ))
                .primaryKey(new TimescaleColumn[]{
                        TimescaleColumn.builder()
                                .name("Id")
                                .dataType(TimescaleColumn.TimescaleDataType.SERIAL)
                                .relatedDescriptors(Collections.emptyList())
                                .build()
                })
                .build();

        String createTableStatement = sqlGenerator.generateCreateTableStatement(migrationTable);
        dataService.executeQuery(createTableStatement);
    }

    public MigrationMetadata save(MigrationMetadata metadata) {
        return dataService.saveMigrationMetadata(metadata);
    }

    public Optional<MigrationMetadata> getByStreamName(String streamName) {
        return dataService.getMigrationMetadata(streamName);
    }

    public void update(MigrationMetadata metadata) {
        dataService.updateMigrationMetadata(metadata);
    }
}

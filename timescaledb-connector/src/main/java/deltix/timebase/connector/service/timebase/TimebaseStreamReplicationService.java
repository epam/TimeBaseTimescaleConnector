package deltix.timebase.connector.service.timebase;

import deltix.gflog.Log;
import deltix.gflog.LogFactory;
import deltix.qsrv.hf.pub.RawMessage;
import deltix.qsrv.hf.tickdb.pub.DXTickStream;
import deltix.timebase.connector.event.FailedReplicationEvent;
import deltix.timebase.connector.model.MigrationMetadata;
import deltix.timebase.connector.model.schema.TimescaleSchema;
import deltix.timebase.connector.model.system.StreamMetaData;
import deltix.timebase.connector.service.DataFeeder;
import deltix.timebase.connector.service.MigrationService;
import deltix.timebase.connector.service.ReplicationService;
import deltix.timebase.connector.service.timescale.TimescaleDataService;
import deltix.timebase.connector.service.timescale.TimescaleMigrationMetadataService;
import deltix.timebase.connector.service.timescale.TimescaleSchemaDefinition;
import deltix.timebase.connector.service.timescale.TimescaleSqlGenerator;
import deltix.timebase.messages.schema.SchemaChangeMessage;
import lombok.AllArgsConstructor;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

@Service
@AllArgsConstructor
public class TimebaseStreamReplicationService implements ReplicationService<String> {

    private static final Log LOG = LogFactory.getLog(TimebaseStreamReplicationService.class);

    private final TimescaleDataService timescaleDataService;
    private final TimebaseConnectionService connectionService;
    private final TimescaleSchemaDefinition timebaseSchemaDefinition;
    private final TimescaleSqlGenerator timescaleSqlGenerator;
    private final TimescaleMigrationMetadataService migrationMetadataService;
    private final MigrationService<SchemaChangeMessage> migrationService;
    private final ApplicationEventPublisher eventPublisher;
    private final StreamMetaDataCacheService streamMetaDataCacheService;

    public void replicate(String streamName) {
        try {
            DXTickStream stream = connectionService.getStream(streamName);

            TimescaleSchema schema = timebaseSchemaDefinition.getTimebaseSchemaDefinition(stream);

            generateHyperTable(schema);

            if (!migrationMetadataService.getByStreamName(streamName).isPresent()) {
                generateMigrationMetadata(streamName, stream.getDataVersion());
            } else {
                // TODO handle version mismatch
            }

            long lastTimestamp = getLastReplicatedTimestamp(schema);

            String insertQuery = generateInsertStatement(schema);

            transferData(stream, lastTimestamp, insertQuery, schema);
        } catch (Exception ex) {
            LOG.error().append("Failed to replicate stream: ").append(streamName).append(ex).commit();
            StreamMetaData metaData = streamMetaDataCacheService.get(streamName);
            metaData.updateStatus(StreamMetaData.Status.FAILED);
            eventPublisher.publishEvent(new FailedReplicationEvent(streamName));
        }
    }

    protected void generateHyperTable(TimescaleSchema schema) {
        String createTableStatement = timescaleSqlGenerator.generateCreateTableStatement(schema);

        LOG.info().append("Generated CREATE TABLE STATEMENT: ").append(createTableStatement).commit();

        timescaleDataService.executeQuery(createTableStatement);

        LOG.info().append("Table ").append(schema.getName()).append(" created").commit();

        String hypertableStatement = timescaleSqlGenerator.generateHypertableStatement(schema);

        LOG.info().append("Generated HYPERTABLE STATEMENT: ").append(hypertableStatement).commit();

        timescaleDataService.executeQuery(hypertableStatement);

        LOG.info().append("Hypertable created").commit();
    }

    protected String generateInsertStatement(TimescaleSchema schema) {
        String insertQuery = timescaleSqlGenerator.generateInsertStatement(schema);

        LOG.info().append("Generated INSERT STATEMENT: ").append(insertQuery).commit();

        return insertQuery;
    }

    protected long getLastReplicatedTimestamp(TimescaleSchema schema) {
        String lastTimestampStatement = timescaleSqlGenerator.generateLastTimestampStatement(schema.getName());

        LOG.info().append("Generated last timestamp statement: ").append(lastTimestampStatement).commit();

        long lastTimestamp = timescaleDataService.getLastTimestamp(lastTimestampStatement);

        LOG.info().append("Last timestamp found: ").append(Instant.ofEpochMilli(lastTimestamp)).commit();

        if (lastTimestamp > 0) {
            String deleteLastTimestampStatement = timescaleSqlGenerator.generateDeleteLastTimestampStatement(schema.getName());

            LOG.info().append("Generated delete last timestamp statement: ").append(deleteLastTimestampStatement).commit();

            timescaleDataService.deleteLastTimestampData(deleteLastTimestampStatement, lastTimestamp);
        }

        return lastTimestamp;
    }

    private void transferData(DXTickStream stream, long lastTimestamp, String insertQuery, TimescaleSchema schema) {
        DataFeeder<RawMessage> dataFeeder = new RawMessageDataFeeder(stream, lastTimestamp, migrationService);

        try {
            while (true) {
                List<RawMessage> messages = dataFeeder.fetchData(500);

                if (!messages.isEmpty()) {
                    LOG.info().append("Try to save: ").append(messages.size()).append(" raws from stream - ").append(stream.getName()).commit();

                    timescaleDataService.insertBatch(insertQuery, messages, schema);
                } else {
                    Thread.currentThread().sleep(500l);
                }
            }
        } catch (Exception ex) {
            dataFeeder.close();

            throw new RuntimeException(ex);
        }
    }

    private MigrationMetadata generateMigrationMetadata(String streamName, long version) {
        MigrationMetadata metadata = MigrationMetadata.builder()
                .dateTime(System.currentTimeMillis())
                .isSuccess(Boolean.TRUE)
                .stream(streamName)
                .version(version)
                .build();

        return migrationMetadataService.save(metadata);
    }
}

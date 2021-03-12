package deltix.timebase.connector;

import deltix.gflog.Log;
import deltix.gflog.LogFactory;
import deltix.timebase.connector.event.FailedReplicationEvent;
import deltix.timebase.connector.event.NewStreamEvent;
import deltix.timebase.connector.model.system.StreamMetaData;
import deltix.timebase.connector.service.timebase.StreamMetaDataCacheService;
import deltix.timebase.connector.service.timebase.TimebaseStreamDiscoveryService;
import deltix.timebase.connector.service.timebase.TimebaseStreamReplicationService;
import deltix.timebase.connector.service.timescale.TimescaleMigrationMetadataService;
import deltix.timebase.connector.util.DiscoveryUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Component
public class ApplicationEventHandler {

    private static final Log LOG = LogFactory.getLog(ApplicationEventHandler.class);

    private List<String> streams;
    private TimebaseStreamReplicationService replicationService;
    private ExecutorService executor;
    private TimebaseStreamDiscoveryService discoveryService;
    private StreamMetaDataCacheService cacheService;
    private Integer retryAttempts;
    private TimescaleMigrationMetadataService migrationMetadataService;

    private List<String> streamWildcards;

    @Autowired
    public ApplicationEventHandler(@Qualifier("streamNames") List<String> streams,
                                   TimebaseStreamReplicationService replicationService,
                                   TimebaseStreamDiscoveryService discoveryService,
                                   StreamMetaDataCacheService cacheService,
                                   @Value("${replicator.retryAttempts}") Integer retryAttempts,
                                   TimescaleMigrationMetadataService migrationMetadataService) {
        this.streams = streams;
        this.replicationService = replicationService;
        this.discoveryService = discoveryService;
        this.cacheService = cacheService;
        this.retryAttempts = retryAttempts;
        this.migrationMetadataService = migrationMetadataService;
        this.executor = Executors.newCachedThreadPool();
    }

    @EventListener
    public void handleApplicationStartUpEvent(ApplicationReadyEvent event) {
        LOG.info().append("Start handling timebase streams: ").append(streams).commit();

        migrationMetadataService.createMigrationTable();

        streamWildcards = streams.stream()
                .filter(DiscoveryUtils::isExpression)
                .map(DiscoveryUtils::generateRegExp)
                .collect(Collectors.toList());

        Set<String> initStreams = streams.stream()
                .filter(stream -> !DiscoveryUtils.isExpression(stream))
                .collect(Collectors.toSet());

        List<String> discoveredStreams = streamWildcards.stream()
                .map(discoveryService::discover)
                .flatMap(List::stream)
                .distinct()
                .collect(Collectors.toList());

        initStreams.addAll(discoveredStreams);

        initStreams.forEach(stream -> {
            cacheService.add(stream, new StreamMetaData(StreamMetaData.Status.RUNNING));
            executor.execute(() -> replicationService.replicate(stream));
        });
    }

    @EventListener
    public void handleFailedReplicationEvent(FailedReplicationEvent event) {
        String streamName = (String) event.getSource();
        LOG.info().append("Received failed replication event for stream: ").append(streamName).commit();

        StreamMetaData metaData = cacheService.get(streamName);
        Integer failedAttempts = metaData.getFailedAttempts();

        if (failedAttempts < retryAttempts) {
            executor.execute(() -> replicationService.replicate(streamName));
        } else {
            LOG.error().append("Max replication attempts reached for stream: ").append(streamName).commit();
        }
    }

    @EventListener
    public void handleNewStreamEvent(NewStreamEvent event) {
        String streamName = (String) event.getSource();
        LOG.info().append("Received NewStreamEvent for stream: ").append(streamName).commit();

        boolean isMatched = streamWildcards.stream()
                .filter(wildcard -> streamName.matches(wildcard))
                .findAny()
                .isPresent();

        if (isMatched) {
            cacheService.add(streamName, new StreamMetaData(StreamMetaData.Status.RUNNING));
            executor.execute(() -> replicationService.replicate(streamName));
        }
    }
}

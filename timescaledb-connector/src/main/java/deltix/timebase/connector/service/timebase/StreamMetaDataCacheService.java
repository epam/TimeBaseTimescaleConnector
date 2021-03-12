package deltix.timebase.connector.service.timebase;

import deltix.gflog.Log;
import deltix.gflog.LogFactory;
import deltix.timebase.connector.model.system.StreamMetaData;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Service
public class StreamMetaDataCacheService {

    private static final Log LOG = LogFactory.getLog(StreamMetaDataCacheService.class);

    private ConcurrentMap<String, StreamMetaData> cache = new ConcurrentHashMap<>();

    public void add(String stream, StreamMetaData metaData) {
        cache.putIfAbsent(stream, metaData);
    }

    public StreamMetaData get(String stream) {
        return cache.get(stream);
    }

    public boolean contains(String stream) {
        return cache.containsKey(stream);
    }
}

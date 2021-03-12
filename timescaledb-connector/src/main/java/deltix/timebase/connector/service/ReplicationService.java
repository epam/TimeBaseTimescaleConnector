package deltix.timebase.connector.service;

public interface ReplicationService<T> {

    void replicate(T identity);
}

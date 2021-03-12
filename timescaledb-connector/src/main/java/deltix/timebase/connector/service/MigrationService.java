package deltix.timebase.connector.service;

public interface MigrationService<T> {

    void apply(T model, String schemaName);
}

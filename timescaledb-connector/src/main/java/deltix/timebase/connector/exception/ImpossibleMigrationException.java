package deltix.timebase.connector.exception;

public class ImpossibleMigrationException extends RuntimeException {
    public ImpossibleMigrationException() {
        super();
    }

    public ImpossibleMigrationException(String message) {
        super(message);
    }

    public ImpossibleMigrationException(String message, Throwable cause) {
        super(message, cause);
    }
}

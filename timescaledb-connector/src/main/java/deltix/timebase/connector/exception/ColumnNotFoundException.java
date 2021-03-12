package deltix.timebase.connector.exception;

public class ColumnNotFoundException extends RuntimeException {

    public ColumnNotFoundException() {
        super();
    }

    public ColumnNotFoundException(String message) {
        super(message);
    }

    public ColumnNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}

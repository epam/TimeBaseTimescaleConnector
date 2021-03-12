package deltix.timebase.connector.model.system;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class StreamMetaData {
    private AtomicInteger failedAttempts;
    private AtomicReference<Status> status;
    private Instant updateTime;

    public StreamMetaData(Status status) {
        this.status = new AtomicReference<>(status);
        this.updateTime = Instant.now();
        this.failedAttempts = new AtomicInteger();
    }

    public void updateStatus(Status status) {
        this.status.getAndSet(status);
        this.updateTime = Instant.now();
        if (status.equals(Status.FAILED)) {
            failedAttempts.incrementAndGet();
        }
    }

    public Integer getFailedAttempts() {
        return failedAttempts.get();
    }

    public enum Status {
        RUNNING, FAILED
    }
}

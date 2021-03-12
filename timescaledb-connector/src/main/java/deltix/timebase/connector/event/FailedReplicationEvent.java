package deltix.timebase.connector.event;

import org.springframework.context.ApplicationEvent;

public class FailedReplicationEvent extends ApplicationEvent {

    public FailedReplicationEvent(String streamName) {
        super(streamName);
    }
}

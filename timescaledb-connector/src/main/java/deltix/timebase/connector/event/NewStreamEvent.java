package deltix.timebase.connector.event;

import org.springframework.context.ApplicationEvent;

public class NewStreamEvent extends ApplicationEvent {

    public NewStreamEvent(String stream) {
        super(stream);
    }
}

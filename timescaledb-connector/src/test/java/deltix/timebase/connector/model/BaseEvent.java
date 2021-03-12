package deltix.timebase.connector.model;

import deltix.timebase.messages.InstrumentMessage;
import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;

public abstract class BaseEvent extends InstrumentMessage {

    private long dateTime;
    private String name;
    private boolean isFinal;
    private EventType eventType;

    @SchemaType(
            dataType = SchemaDataType.TIMESTAMP
    )
    @SchemaElement
    public long getDateTime() {
        return dateTime;
    }

    public void setDateTime(long dateTime) {
        this.dateTime = dateTime;
    }

    @SchemaElement
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @SchemaElement
    public boolean isFinal() {
        return isFinal;
    }

    public void setFinal(boolean aFinal) {
        isFinal = aFinal;
    }

    @SchemaElement
    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }
}

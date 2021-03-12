package deltix.timebase.connector.model;

import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;

public class TimebaseEvent extends BaseEvent {

    private long value;
    private boolean isGlobal;

    @SchemaType(
            encoding = "INT64",
            dataType = SchemaDataType.INTEGER
    )
    @SchemaElement
    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    @SchemaElement
    public boolean isGlobal() {
        return isGlobal;
    }

    public void setGlobal(boolean global) {
        isGlobal = global;
    }
}

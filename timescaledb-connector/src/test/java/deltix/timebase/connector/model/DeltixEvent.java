package deltix.timebase.connector.model;

import deltix.dfp.Decimal64;
import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;

public class DeltixEvent extends BaseEvent {

    private Decimal64 quantity;
    private OwnerEntity owner;
    private String reporter;

    @SchemaType(
            encoding = "DECIMAL64",
            dataType = SchemaDataType.FLOAT
    )
    @SchemaElement
    public Decimal64 getQuantity() {
        return quantity;
    }

    public void setQuantity(Decimal64 quantity) {
        this.quantity = quantity;
    }

    @SchemaElement
    public OwnerEntity getOwner() {
        return owner;
    }

    public void setOwner(OwnerEntity owner) {
        this.owner = owner;
    }

    @SchemaElement
    public String getReporter() {
        return reporter;
    }

    public void setReporter(String reporter) {
        this.reporter = reporter;
    }
}

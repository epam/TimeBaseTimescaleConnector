package deltix.timebase.connector.model;

import deltix.dfp.Decimal64;
import deltix.timebase.messages.InstrumentMessage;
import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;
import deltix.util.collections.generated.ByteArrayList;

public abstract class BaseTestClass extends InstrumentMessage {

    private CharSequence stringValue;
    private Decimal64 decimal64Value;
    private ByteArrayList byteaValue;

    @SchemaElement
    public CharSequence getStringValue() {
        return stringValue;
    }

    public void setStringValue(CharSequence stringValue) {
        this.stringValue = stringValue;
    }

    @SchemaType(
            dataType = SchemaDataType.FLOAT,
            encoding = "DECIMAL64"
    )
    @SchemaElement
    public Decimal64 getDecimal64Value() {
        return decimal64Value;
    }

    public void setDecimal64Value(Decimal64 decimal64Value) {
        this.decimal64Value = decimal64Value;
    }

    @SchemaElement
    public ByteArrayList getByteaValue() {
        return byteaValue;
    }

    public void setByteaValue(ByteArrayList byteaValue) {
        this.byteaValue = byteaValue;
    }
}

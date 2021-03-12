package deltix.timebase.connector.model;

import deltix.qsrv.hf.pub.md.IntegerDataType;
import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;

public class FirstTestClass extends BaseTestClass {

    private char charValue;
    private int intValue;
    private long longValue;
    private InnerTestClass innerState;

    @SchemaElement
    public InnerTestClass getInnerState() {
        return innerState;
    }

    public void setInnerState(InnerTestClass innerState) {
        this.innerState = innerState;
    }

    @SchemaType(
            dataType = SchemaDataType.CHAR
    )
    @SchemaElement
    public char getCharValue() {
        return charValue;
    }

    public void setCharValue(char charValue) {
        this.charValue = charValue;
    }

    @SchemaType(
            encoding = IntegerDataType.ENCODING_INT32,
            dataType = SchemaDataType.INTEGER
    )
    @SchemaElement
    public int getIntValue() {
        return intValue;
    }

    public void setIntValue(int intValue) {
        this.intValue = intValue;
    }

    @SchemaType(
            encoding = IntegerDataType.ENCODING_INT64,
            dataType = SchemaDataType.INTEGER
    )
    @SchemaElement
    public long getLongValue() {
        return longValue;
    }

    public void setLongValue(long longValue) {
        this.longValue = longValue;
    }

    @Override
    public String toString() {
        return "FirstTestClass{" +
                "charValue=" + charValue +
                ", intValue=" + intValue +
                ", longValue=" + longValue +
                ", stringValue=" + getStringValue() +
                ", decimal64Value=" + getDecimal64Value() +
                ", byteaValue=" + getByteaValue() +
                ", timestamp=" + timestamp +
                ", nanoTime=" + nanoTime +
                ", symbol=" + symbol +
                '}';
    }
}

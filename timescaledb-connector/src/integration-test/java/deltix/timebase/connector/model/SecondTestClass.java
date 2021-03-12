package deltix.timebase.connector.model;

import deltix.qsrv.hf.pub.md.IntegerDataType;
import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;

public class SecondTestClass extends BaseTestClass {

    private String secondValue1;
    private long secondLongValue;
    private SomeTestEnum enumValue;

    @SchemaType(
            dataType = SchemaDataType.ENUM
    )
    @SchemaElement
    public SomeTestEnum getEnumValue() {
        return enumValue;
    }

    public void setEnumValue(SomeTestEnum enumValue) {
        this.enumValue = enumValue;
    }

    @SchemaElement
    public String getSecondValue1() {
        return secondValue1;
    }

    public void setSecondValue1(String secondValue1) {
        this.secondValue1 = secondValue1;
    }

    @SchemaType(
            encoding = IntegerDataType.ENCODING_INT64,
            dataType = SchemaDataType.INTEGER
    )
    @SchemaElement
    public long getSecondLongValue() {
        return secondLongValue;
    }

    public void setSecondLongValue(long secondLongValue) {
        this.secondLongValue = secondLongValue;
    }

    @Override
    public String toString() {
        return "SecondTestClass{" +
                "secondValue1='" + secondValue1 + '\'' +
                ", secondLongValue=" + secondLongValue +
                ", enumValue=" + enumValue +
                ", stringValue=" + getStringValue() +
                ", decimal64Value=" + getDecimal64Value() +
                ", byteaValue=" + getByteaValue() +
                ", timestamp=" + timestamp +
                ", nanoTime=" + nanoTime +
                ", symbol=" + symbol +
                '}';
    }
}

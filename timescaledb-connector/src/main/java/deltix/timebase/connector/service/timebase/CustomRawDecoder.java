package deltix.timebase.connector.service.timebase;

import deltix.qsrv.hf.pub.NullValueException;
import deltix.qsrv.hf.pub.RawDecoder;
import deltix.qsrv.hf.pub.ReadableValue;
import deltix.qsrv.hf.pub.codec.NonStaticFieldInfo;
import deltix.qsrv.hf.pub.codec.UnboundDecoder;
import deltix.qsrv.hf.pub.md.*;

import java.util.LinkedHashMap;
import java.util.Map;

import static deltix.qsrv.hf.pub.RawMessageManipulator.OBJECT_CLASS_NAME;

public class CustomRawDecoder extends RawDecoder {

    @Override
    public Object readField(DataType type, ReadableValue rv) {
        try {
            if (type instanceof IntegerDataType) {
                return readInteger((IntegerDataType) type, rv);
            } else if (type instanceof FloatDataType)
                return readFloat((FloatDataType) type, rv);
            else if (type instanceof CharDataType)
                return rv.getChar();
            else if (type instanceof EnumDataType || type instanceof VarcharDataType)
                return rv.getString();
            else if (type instanceof BooleanDataType)
                return readBoolean((BooleanDataType)type, rv);
            else if (type instanceof DateTimeDataType)
                return readDateTime((DateTimeDataType)type, rv);
            else if (type instanceof TimeOfDayDataType)
                return rv.getInt();
            else if (type instanceof ArrayDataType)
                return readArray((ArrayDataType) type, rv);
            else if (type instanceof ClassDataType)
                return readObjectValues(rv);
            else if (type instanceof BinaryDataType) {
                try {
                    final int size = rv.getBinaryLength();
                    final byte[] bin = new byte[size];
                    rv.getBinary(0, size, bin, 0);
                    return bin;
                } catch (NullValueException e) {
                    return null;
                }
            } else
                throw new RuntimeException("Unrecognized dataType: " + type);
        } catch (NullValueException e) {
            return null;
        }
    }

    @Override
    protected Object readFloat(FloatDataType type, ReadableValue rv) {
        if (type.isDecimal64()) {
            return rv.getLong();
        } else {
            return super.readFloat(type, rv);
        }
    }

    private Object[] readArray(ArrayDataType type, ReadableValue udec) throws NullValueException {
        final int len = udec.getArrayLength();
        final DataType elementType = type.getElementDataType();

        final boolean isNullableBool = (elementType instanceof BooleanDataType) && elementType.isNullable();

        Object[] values = new Object[len];

        for (int i = 0; i < len; i++) {
            Object value;
            try {
                final ReadableValue rv = udec.nextReadableElement();
                value = readField(elementType, rv);
            } catch (NullValueException e) {
                value = null;
            }

            if (isNullableBool) {
                Boolean b = (Boolean) value;
                values[i] = (byte) (b == null ? -1 : b ? 1 : 0);
            } else {
                values[i] = value;
            }

        }
        return values;
    }

    private Map<String, Object> readObjectValues(ReadableValue udec) throws NullValueException {
        final UnboundDecoder decoder = udec.getFieldDecoder();
        Map<String, Object> values = new LinkedHashMap<>();

        if (decoder.getClassInfo() != null)
            values.put(OBJECT_CLASS_NAME, decoder.getClassInfo().getDescriptor());

        // dump field/value pairs
        while (decoder.nextField()) {
            NonStaticFieldInfo field = decoder.getField();
            Object value = readField(field.getType(), decoder);
            values.put(field.getName(), value);
        }

        return values;
    }
}

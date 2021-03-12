package deltix.timebase.connector.service.timebase;

import deltix.qsrv.hf.pub.RawDecoder;
import deltix.qsrv.hf.pub.RawMessage;
import deltix.qsrv.hf.pub.ReadableValue;
import deltix.qsrv.hf.pub.codec.CodecFactory;
import deltix.qsrv.hf.pub.codec.NonStaticFieldInfo;
import deltix.qsrv.hf.pub.codec.UnboundDecoder;
import deltix.qsrv.hf.pub.md.DataType;
import deltix.qsrv.hf.pub.md.RecordClassDescriptor;
import deltix.util.memory.MemoryDataInput;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class TimebaseRawMessageService {

    private static final RawDecoder RAW_DECODER = new CustomRawDecoder();

    public Map<String, Object> expandValues(RawMessage message) {
        if (message == null) {
            throw new IllegalArgumentException("RawMessage could not be null");
        }

        Map<String, Object> values = getValues(message);

        return expandValues(values, null);
    }

    private Map<String, Object> getValues(RawMessage message) {
        Map<String, Object> values = new HashMap<>();

        String messageDescriptor = message.type.getName();

        values.put("descriptor_name", messageDescriptor);

        RecordClassDescriptor type = message.type;
        UnboundDecoder decoder = CodecFactory.COMPILED.createFixedUnboundDecoder(type);

        MemoryDataInput in = new MemoryDataInput();
        in.setBytes(message.data, message.offset, message.length);
        decoder.beginRead(in);

        while (decoder.nextField()) {
            NonStaticFieldInfo field = decoder.getField();
            DataType dataType = field.getType();

            Object objectValue = getValue(dataType, decoder);

            values.put(field.getName(), objectValue);
        }

        return values;
    }

    private Object getValue(DataType type, ReadableValue value) {
        return RAW_DECODER.readField(type, value);
    }

    private Map<String, Object> expandValues(Map<String, Object> values, String parentFieldName) {
        Map<String, Object> expandedValues = new HashMap<>();

        if (parentFieldName != null) {
            RecordClassDescriptor classDescriptor = (RecordClassDescriptor) values.get("objectClassName");
            expandedValues.put(parentFieldName + "_" + "descriptor_name", classDescriptor.getName());
        }

        values.forEach((name, value) -> {
            String fieldName = parentFieldName == null ? name : parentFieldName + "_" + name;
            if (value instanceof Map) {
                expandedValues.putAll(expandValues((Map<String, Object>) value, fieldName));
            } else {
                expandedValues.put(fieldName, value);
            }
        });

        return expandedValues;
    }
}

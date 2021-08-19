/*
 * Copyright 2021 EPAM Systems, Inc
 *
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. Licensed under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.epam.deltix.timebase.connector.service.timebase;

import com.epam.deltix.qsrv.hf.pub.RawDecoder;
import com.epam.deltix.qsrv.hf.pub.RawMessage;
import com.epam.deltix.qsrv.hf.pub.ReadableValue;
import com.epam.deltix.qsrv.hf.pub.codec.CodecFactory;
import com.epam.deltix.qsrv.hf.pub.codec.NonStaticFieldInfo;
import com.epam.deltix.qsrv.hf.pub.codec.UnboundDecoder;
import com.epam.deltix.qsrv.hf.pub.md.DataType;
import com.epam.deltix.qsrv.hf.pub.md.RecordClassDescriptor;
import com.epam.deltix.util.memory.MemoryDataInput;
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

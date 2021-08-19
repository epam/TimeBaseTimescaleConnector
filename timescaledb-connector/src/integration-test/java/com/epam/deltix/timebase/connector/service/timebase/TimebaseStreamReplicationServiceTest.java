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

import com.epam.deltix.dfp.Decimal64;
import com.epam.deltix.qsrv.hf.pub.RawMessage;
import com.epam.deltix.qsrv.hf.pub.md.Introspector;
import com.epam.deltix.qsrv.hf.pub.md.RecordClassDescriptor;
import com.epam.deltix.qsrv.hf.tickdb.pub.*;
import com.epam.deltix.timebase.connector.model.*;
import com.epam.deltix.timebase.connector.model.schema.TimescaleSchema;
import com.epam.deltix.timebase.connector.service.BaseServiceIntegrationTest;
import com.epam.deltix.timebase.connector.service.DataFeeder;
import com.epam.deltix.timebase.connector.service.MigrationService;
import com.epam.deltix.timebase.connector.service.timescale.TimescaleDataService;
import com.epam.deltix.timebase.connector.service.timescale.TimescaleSchemaDefinition;
import com.epam.deltix.timebase.messages.TimeStampedMessage;
import com.epam.deltix.timebase.messages.schema.SchemaChangeMessage;
import com.epam.deltix.util.collections.generated.ByteArrayList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;

public class TimebaseStreamReplicationServiceTest extends BaseServiceIntegrationTest {

    @Autowired
    private TimebaseConnectionService connectionService;

    @Autowired
    private TimebaseStreamReplicationService timebaseStreamReplicationService;

    @Autowired
    private TimescaleSchemaDefinition timescaleSchemaDefinition;

    @Autowired
    private TimescaleDataService dataService;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private MigrationService<SchemaChangeMessage> migrationService;

    @BeforeEach
    protected void initTimeBaseStream() {
        DXTickDB tbConnection = connectionService.getConnection();
        connectionService.init(false);

        Introspector introspector = Introspector.createEmptyMessageIntrospector();

        RecordClassDescriptor[] recordClassDescriptors = new RecordClassDescriptor[2];
        try {
            RecordClassDescriptor firstRecordClassDescriptor = introspector.introspectRecordClass(FirstTestClass.class);
            RecordClassDescriptor secondRecordClassDescriptor = introspector.introspectRecordClass(SecondTestClass.class);
            recordClassDescriptors[0] = firstRecordClassDescriptor;
            recordClassDescriptors[1] = secondRecordClassDescriptor;
        } catch (Introspector.IntrospectionException ex) {
            ex.printStackTrace();
        }

        StreamOptions streamOptions = new StreamOptions();
        streamOptions.setPolymorphic(recordClassDescriptors);

        DXTickStream someStream = tbConnection.createStream("someStreamName", streamOptions);

        LoadingOptions loadingOptions = new LoadingOptions(LoadingOptions.WriteMode.APPEND);
        TickLoader loader = someStream.createLoader(loadingOptions);

        List<BaseTestClass> entities = getTimeBaseEntities();

        entities.forEach(entity -> loader.send(entity));

        loader.close();
    }

    @Test
    public void testTimebaseStreamReplication() {
        // open stream in readOnly mode
        DXTickStream stream = connectionService.getStream("someStreamName");
        // generate schema
        TimescaleSchema schema = timescaleSchemaDefinition.getTimebaseSchemaDefinition(stream);

        // apply generated schema to TimescaleDB
        timebaseStreamReplicationService.generateHyperTable(schema);
        // get last message timestamp
        long lastReplicatedTimestamp = timebaseStreamReplicationService.getLastReplicatedTimestamp(schema);
        // generate insert query
        String insertQuery = timebaseStreamReplicationService.generateInsertStatement(schema);

        // create TimeBase data feeder
        DataFeeder<RawMessage> dataFeeder = new RawMessageDataFeeder(stream, lastReplicatedTimestamp, migrationService);

        // start replication
        int replicatedMessageCount = 0;
        while (replicatedMessageCount < 2) {
            List<RawMessage> messages = dataFeeder.fetchData(500);

            if (!messages.isEmpty()) {
                dataService.insertBatch(insertQuery, messages, schema);
                replicatedMessageCount += messages.size();
            }
        }

        // get initial TimeBase entities
        List<BaseTestClass> timeBaseEntities = getTimeBaseEntities();
        // get replicated entities from TimescaleDB
        List<BaseTestClass> timescaleEntities = getTimescaleEntities();

        // assert entities
        assertFirstTestClass(timeBaseEntities, timescaleEntities);
        assertSecondTestClass(timeBaseEntities, timescaleEntities);
    }

    private List<BaseTestClass> getTimeBaseEntities() {
        List<BaseTestClass> entities = new ArrayList<>();

        FirstTestClass firstMessage = new FirstTestClass();
        firstMessage.setCharValue('c');
        firstMessage.setIntValue(27);
        firstMessage.setLongValue(2l);
        firstMessage.setByteaValue(new ByteArrayList("Some bytes".getBytes()));
        firstMessage.setStringValue("some string");
        firstMessage.setDecimal64Value(Decimal64.fromDouble(123456.789564321d));
        firstMessage.setSymbol("firstsymbol");
        firstMessage.setInnerState(new InnerTestClass("Name", "Value"));
        firstMessage.setTimeStampMs(TimeStampedMessage.TIMESTAMP_UNKNOWN);

        entities.add(firstMessage);

        SecondTestClass secondMessage = new SecondTestClass();
        secondMessage.setEnumValue(SomeTestEnum.ONE);
        secondMessage.setSecondLongValue(4l);
        secondMessage.setSecondValue1("second string value");
        secondMessage.setByteaValue(new ByteArrayList("second value bytes".getBytes()));
        secondMessage.setDecimal64Value(Decimal64.fromDouble(78.123654789d));
        secondMessage.setStringValue("string value 2");
        secondMessage.setSymbol("secondsymbol");
        secondMessage.setTimeStampMs(TimeStampedMessage.TIMESTAMP_UNKNOWN);

        entities.add(secondMessage);

        return entities;
    }

    private List<BaseTestClass> getTimescaleEntities() {
        return jdbcTemplate.query("select * from \"someStreamName\"", (rs, rowNum) -> {
            String descriptorName = rs.getString("descriptor_name");
            String firstTestClass_charValue = rs.getString("charValue");
            int firstTestClass_intValue = rs.getInt("intValue");
            long firstTestClass_longValue = rs.getLong("longValue");
            byte[] baseTestClass_byteaValues = rs.getBytes("byteaValue");
            BigDecimal baseTestClass_decimal64Value = rs.getBigDecimal("decimal64Value");
            String baseTestClass_stringValue = rs.getString("stringValue");
            String secondTestClass_enumValue = rs.getString("enumValue");
            long secondTestClass_secondLongValue = rs.getLong("secondLongValue");
            String secondTestClass_secondValue1 = rs.getString("secondValue1");
            Timestamp eventTime = rs.getTimestamp("EventTime");
            String symbol = rs.getString("Symbol");
            String innerName = rs.getString("innerState_name");
            String innerValue = rs.getString("innerState_value");

            if (descriptorName.equals("com.epam.deltix.timebase.connector.model.FirstTestClass")) {
                FirstTestClass entity = new FirstTestClass();

                if (innerName != null && innerValue != null) {
                    entity.setInnerState(new InnerTestClass(innerName, innerValue));
                }

                if (firstTestClass_charValue != null) {
                    entity.setCharValue(firstTestClass_charValue.charAt(0));
                }

                if (firstTestClass_intValue != 0) {
                    entity.setIntValue(firstTestClass_intValue);
                }

                if (firstTestClass_longValue != 0) {
                    entity.setLongValue(firstTestClass_longValue);
                }

                if (baseTestClass_byteaValues != null) {
                    entity.setByteaValue(new ByteArrayList(baseTestClass_byteaValues));
                }

                if (baseTestClass_decimal64Value != null) {
                    entity.setDecimal64Value(Decimal64.fromBigDecimal(baseTestClass_decimal64Value));
                }

                if (baseTestClass_stringValue != null) {
                    entity.setStringValue(baseTestClass_stringValue);
                }

                if (eventTime != null) {
                    entity.setTimeStampMs(eventTime.getTime());
                }

                if (symbol != null) {
                    entity.setSymbol(symbol);
                }

                return entity;
            } else {
                SecondTestClass entity = new SecondTestClass();

                if (secondTestClass_enumValue != null) {
                    entity.setEnumValue(SomeTestEnum.valueOf(secondTestClass_enumValue));
                }

                if (secondTestClass_secondLongValue != 0) {
                    entity.setSecondLongValue(secondTestClass_secondLongValue);
                }

                if (secondTestClass_secondValue1 != null) {
                    entity.setSecondValue1(secondTestClass_secondValue1);
                }

                if (baseTestClass_byteaValues != null) {
                    entity.setByteaValue(new ByteArrayList(baseTestClass_byteaValues));
                }

                if (baseTestClass_decimal64Value != null) {
                    entity.setDecimal64Value(Decimal64.fromBigDecimal(baseTestClass_decimal64Value));
                }

                if (baseTestClass_stringValue != null) {
                    entity.setStringValue(baseTestClass_stringValue);
                }

                if (eventTime != null) {
                    entity.setTimeStampMs(eventTime.getTime());
                }

                if (symbol != null) {
                    entity.setSymbol(symbol);
                }

                return entity;
            }
        });
    }

    /*@AfterEach
    protected void destroyStream() {
        DXTickDB tbConnection = connectionService.getConnection();
        DXTickStream stream = tbConnection.getStream("someStreamName");
        stream.delete();
    }*/

    private void assertFirstTestClass(List<BaseTestClass> timeBaseEntities, List<BaseTestClass> timescaleEntities) {
        FirstTestClass timebaseFirstTestClass = (FirstTestClass) timeBaseEntities.stream()
                .filter(entity -> entity instanceof FirstTestClass)
                .findAny()
                .get();

        FirstTestClass timescaleFirstTestClass = (FirstTestClass) timescaleEntities.stream()
                .filter(entity -> entity instanceof FirstTestClass)
                .findAny()
                .get();

        assertThat(timebaseFirstTestClass.getByteaValue(), is(timescaleFirstTestClass.getByteaValue()));
        assertThat(timebaseFirstTestClass.getDecimal64Value(), is(timescaleFirstTestClass.getDecimal64Value()));
        assertThat(timebaseFirstTestClass.getStringValue(), is(timescaleFirstTestClass.getStringValue()));
        assertThat(timebaseFirstTestClass.getSymbol(), is(timescaleFirstTestClass.getSymbol()));

        assertEquals(timebaseFirstTestClass.getCharValue(), timescaleFirstTestClass.getCharValue());
        assertEquals(timebaseFirstTestClass.getIntValue(), timescaleFirstTestClass.getIntValue());
        assertEquals(timebaseFirstTestClass.getLongValue(), timescaleFirstTestClass.getLongValue());
    }

    private void assertSecondTestClass(List<BaseTestClass> timeBaseEntities, List<BaseTestClass> timescaleEntities) {
        SecondTestClass timebaseSecondTestClass = (SecondTestClass) timeBaseEntities.stream()
                .filter(entity -> entity instanceof SecondTestClass)
                .findAny()
                .get();

        SecondTestClass timescaleSecondTestClass = (SecondTestClass) timescaleEntities.stream()
                .filter(entity -> entity instanceof SecondTestClass)
                .findAny()
                .get();

        assertThat(timebaseSecondTestClass.getByteaValue(), is(timescaleSecondTestClass.getByteaValue()));
        assertThat(timebaseSecondTestClass.getDecimal64Value(), is(timescaleSecondTestClass.getDecimal64Value()));
        assertThat(timebaseSecondTestClass.getStringValue(), is(timescaleSecondTestClass.getStringValue()));
        assertThat(timebaseSecondTestClass.getSymbol(), is(timescaleSecondTestClass.getSymbol()));

        assertThat(timebaseSecondTestClass.getEnumValue(), is(timescaleSecondTestClass.getEnumValue()));
        assertEquals(timebaseSecondTestClass.getSecondLongValue(), timescaleSecondTestClass.getSecondLongValue());
        assertThat(timebaseSecondTestClass.getSecondValue1(), is(timescaleSecondTestClass.getSecondValue1()));
    }
}

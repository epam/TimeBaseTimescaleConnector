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
package com.epam.deltix.timebase.connector.service.timescale;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.epam.deltix.dfp.Decimal64;
import com.epam.deltix.gflog.api.*;
import com.epam.deltix.qsrv.hf.pub.RawMessage;
import com.epam.deltix.timebase.connector.model.MigrationMetadata;
import com.epam.deltix.timebase.connector.model.schema.TimescaleColumn;
import com.epam.deltix.timebase.connector.model.schema.TimescaleSchema;
import com.epam.deltix.timebase.connector.service.timebase.TimebaseRawMessageService;
import lombok.AllArgsConstructor;
import org.postgresql.util.PGobject;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.sql.*;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
@AllArgsConstructor
public class TimescaleDataService {

    private static final Log LOG = LogFactory.getLog(TimescaleDataService.class);

    private final JdbcTemplate jdbcTemplate;
    private final NamedParameterJdbcTemplate namedJdbcTemplate;
    private final TimebaseRawMessageService rawMessageService;
    private final ObjectMapper mapper;

    public void executeQuery(String query) {
        jdbcTemplate.execute(query);
    }

    public void insertBatch(String query, List<RawMessage> messages, TimescaleSchema schema) {
        List<TimescaleColumn> columns = schema.getColumns()
                .stream()
                .filter(column -> !column.getName().equals("Id"))
                .collect(Collectors.toList());
        jdbcTemplate.batchUpdate(query, new BatchPreparedStatementSetter() {

            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                RawMessage message = messages.get(i);

                Map<String, Object> expandedValues = rawMessageService.expandValues(message);

                Integer position = 1;

                for (TimescaleColumn column : columns) {
                    String name = column.getName();
                    TimescaleColumn.TimescaleDataType dataType = column.getDataType();

                    if (name.equals("EventTime")) {
                        ps.setTimestamp(position, Timestamp.from(Instant.ofEpochMilli(message.getTimeStampMs())));
                    } else if (name.equals("Symbol")) {
                        ps.setString(position,message.getSymbol().toString());
                    }/* else if (name.equals("InstrumentType")) {
                        ps.setString(position, message.getInstrumentType().toString());
                    }*/ else {
                        Object value = expandedValues.get(name);
                        //System.out.println("Start preparing column: " + name);
                        prepareValue(ps, dataType, value, position);
                    }
                    position++;
                }
            }

            @Override
            public int getBatchSize() {
                return messages.size();
            }
        });
    }

    public long getLastTimestamp(String query) {
        return jdbcTemplate.query(query, rs -> {
            if (rs.next()) {
                Timestamp lastEventTime = rs.getTimestamp("EventTime");

                if (lastEventTime != null) {
                    return lastEventTime.getTime();
                } else {
                    return 0l;
                }
            } else {
                return 0l;
            }
        });
    }

    public void deleteLastTimestampData(String query, long timestamp) {
        jdbcTemplate.update(query, ps -> ps.setTimestamp(1, new Timestamp(timestamp)));
    }

    public MigrationMetadata saveMigrationMetadata(MigrationMetadata metadata) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        Map<String, Object> namedParameters = new HashMap<>();
        namedParameters.put("id", metadata.getId());
        namedParameters.put("dateTime", new Timestamp(metadata.getDateTime()));
        namedParameters.put("isSuccess", metadata.getIsSuccess());
        namedParameters.put("version", metadata.getVersion());
        namedParameters.put("stream", metadata.getStream());
        namedJdbcTemplate.update(TimescaleSqlGenerator.INSERT_MIGRATION_METADATA_STATEMENT, new MapSqlParameterSource(namedParameters), keyHolder, new String[] { "id" });
        metadata.setId((int) keyHolder.getKey());

        return metadata;
    }

    public Optional<MigrationMetadata> getMigrationMetadata(String streamName) {
        try {
            return Optional.of(jdbcTemplate.queryForObject(TimescaleSqlGenerator.GET_MIGRATION_METADATA_STATEMENT, (rs, rowNum) ->
                            MigrationMetadata.builder()
                                    .dateTime(rs.getTimestamp("MigrationDateTime").getTime())
                                    .id(rs.getInt("Id"))
                                    .isSuccess(rs.getBoolean("IsSuccess"))
                                    .stream(rs.getString("Stream"))
                                    .version(rs.getLong("Version"))
                                    .build()
                    , streamName));
        } catch (EmptyResultDataAccessException ex) {
            return Optional.empty();
        }
    }

    public void updateMigrationMetadata(MigrationMetadata metadata) {
        Map<String, Object> namedParameters = new HashMap<>();
        namedParameters.put("id", metadata.getId());
        namedParameters.put("dateTime", new Timestamp(metadata.getDateTime()));
        namedParameters.put("isSuccess", metadata.getIsSuccess());
        namedParameters.put("version", metadata.getVersion());
        namedJdbcTemplate.update(TimescaleSqlGenerator.UPDATE_MIGRATION_METADATA_STATEMENT, namedParameters);
    }

    private void prepareValue(PreparedStatement ps, TimescaleColumn.TimescaleDataType dataType, Object value, int position) {
        try {
            switch (dataType) {
                case BYTEA:
                    if (value == null) {
                        ps.setNull(position, Types.BINARY);
                    } else {
                        byte[] bytes = (byte[]) value;
                        ps.setBinaryStream(position, new ByteArrayInputStream(bytes), bytes.length);
                    }
                    break;
                case CHAR:
                    if (value == null) {
                        ps.setNull(position, Types.CHAR);
                    } else {
                        ps.setString(position, value.toString());
                    }
                    break;
                case VARCHAR:
                case UUID:
                    if (value == null) {
                        ps.setNull(position, Types.VARCHAR);
                    } else {
                        ps.setString(position, value.toString());
                    }
                    break;
                case DATE:
                    if (value == null) {
                        ps.setNull(position, Types.DATE);
                    } else {
                        ps.setDate(position, Date.valueOf(value.toString()));
                    }
                    break;
                case DATETIME:
                    if (value == null) {
                        ps.setNull(position, Types.TIMESTAMP);
                    } else {
                        ps.setTimestamp(position, new Timestamp((Long) value));
                    }
                    break;
                case BOOLEAN:
                    if (value == null) {
                        ps.setNull(position, Types.BOOLEAN);
                    } else {
                        ps.setBoolean(position, (Boolean) value);
                    }
                    break;
                case TIME:
                    if (value == null) {
                        ps.setNull(position, Types.TIME);
                    } else {
                        ps.setTime(position, new Time((Long) value));
                    }
                    break;
                case INTEGER:
                    if (value == null) {
                        ps.setNull(position, Types.INTEGER);
                    } else {
                        if (value instanceof Short) {
                            ps.setShort(position, (Short) value);
                        } else if (value instanceof Byte) {
                            ps.setByte(position, (Byte) value);
                        } else {
                            ps.setInt(position, (Integer) value);
                        }
                    }
                    break;
                case LONG:
                    if (value == null) {
                        ps.setNull(position, Types.BIGINT);
                    } else {
                        ps.setLong(position, (Long) value);
                    }
                    break;
                case DECIMAL64:
                    if (value != null) {
                        Decimal64 decimal64 = Decimal64.fromUnderlying((Long) value);

                        if (!decimal64.isNaN() && !decimal64.isInfinity() && !decimal64.isNegativeInfinity()) {
                            ps.setBigDecimal(position, decimal64.toBigDecimal());
                        } else {
                            ps.setNull(position, Types.DECIMAL);
                        }
                    } else {
                        ps.setNull(position, Types.DECIMAL);
                    }
                    break;
                case DECIMAL:
                    if (value == null) {
                        ps.setNull(position, Types.DECIMAL);
                    } else if (value instanceof Float){
                        ps.setFloat(position, (Float) value);
                    } else {
                        ps.setDouble(position, (Double) value);
                    }
                    break;
                case JSON:
                case JSONB:
                    //TODO add jsonb support
                    if (value == null) {
                        ps.setNull(position, Types.OTHER);
                    } else {
                        //jsonb
                        PGobject json = new PGobject();
                        json.setType("json");
                        json.setValue(mapper.writeValueAsString(value));
                        ps.setObject(position, json);
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Timescale data type is not supported: " + dataType);
            }
        } catch (Exception ex) {
            throw new RuntimeException("DataType: " + dataType + " with value: " + value + " value class: " + value.getClass(), ex);
        }
    }
}

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

import com.epam.deltix.gflog.api.*;
import com.epam.deltix.timebase.connector.model.MigrationMetadata;
import com.epam.deltix.timebase.connector.model.schema.TimescaleColumn;
import com.epam.deltix.timebase.connector.model.schema.TimescaleSchema;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

@Service
@AllArgsConstructor
public class TimescaleMigrationMetadataService {

    private static final Log LOG = LogFactory.getLog(TimescaleMigrationMetadataService.class);

    private final TimescaleSqlGenerator sqlGenerator;
    private final TimescaleDataService dataService;

    public void createMigrationTable() {
        TimescaleSchema migrationTable = TimescaleSchema.builder()
                .name("migrations_tracker")
                .columns(Arrays.asList(
                        TimescaleColumn.builder()
                                .name("Id")
                                .dataType(TimescaleColumn.TimescaleDataType.SERIAL)
                                .relatedDescriptors(Collections.emptyList())
                                .build(),
                        TimescaleColumn.builder()
                                .name("Stream")
                                .dataType(TimescaleColumn.TimescaleDataType.VARCHAR)
                                .relatedDescriptors(Collections.emptyList())
                                .build(),
                        TimescaleColumn.builder()
                                .name("Version")
                                .dataType(TimescaleColumn.TimescaleDataType.LONG)
                                .relatedDescriptors(Collections.emptyList())
                                .build(),
                        TimescaleColumn.builder()
                                .name("IsSuccess")
                                .dataType(TimescaleColumn.TimescaleDataType.BOOLEAN)
                                .relatedDescriptors(Collections.emptyList())
                                .build(),
                        TimescaleColumn.builder()
                                .name("MigrationDateTime")
                                .dataType(TimescaleColumn.TimescaleDataType.DATETIME)
                                .relatedDescriptors(Collections.emptyList())
                                .build()
                ))
                .primaryKey(new TimescaleColumn[]{
                        TimescaleColumn.builder()
                                .name("Id")
                                .dataType(TimescaleColumn.TimescaleDataType.SERIAL)
                                .relatedDescriptors(Collections.emptyList())
                                .build()
                })
                .build();

        String createTableStatement = sqlGenerator.generateCreateTableStatement(migrationTable);
        dataService.executeQuery(createTableStatement);
    }

    public MigrationMetadata save(MigrationMetadata metadata) {
        return dataService.saveMigrationMetadata(metadata);
    }

    public Optional<MigrationMetadata> getByStreamName(String streamName) {
        return dataService.getMigrationMetadata(streamName);
    }

    public void update(MigrationMetadata metadata) {
        dataService.updateMigrationMetadata(metadata);
    }
}

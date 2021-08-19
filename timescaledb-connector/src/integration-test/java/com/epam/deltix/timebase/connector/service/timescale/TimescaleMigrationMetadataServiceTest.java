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

import com.epam.deltix.timebase.connector.model.MigrationMetadata;
import com.epam.deltix.timebase.connector.service.BaseServiceIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class TimescaleMigrationMetadataServiceTest extends BaseServiceIntegrationTest {

    @Autowired
    private TimescaleMigrationMetadataService migrationMetadataService;

    @Autowired
    private TimescaleDataService dataService;

    @Test
    public void testCreateMigrationTable() {
        migrationMetadataService.createMigrationTable();

        dataService.executeQuery("select * from migrations_tracker");
    }

    @Test
    public void testSaveMigrationMetadata() {
        migrationMetadataService.createMigrationTable();

        MigrationMetadata metadata = MigrationMetadata.builder()
                .stream("orders")
                .version(7l)
                .isSuccess(Boolean.TRUE)
                .dateTime(0l)
                .build();

        dataService.saveMigrationMetadata(metadata);

        assertThat(metadata.getId(), is(1));
    }

    @Test
    public void testUpdateMigrationMetadata() {
        migrationMetadataService.createMigrationTable();

        MigrationMetadata metadata = MigrationMetadata.builder()
                .stream("orders")
                .version(7l)
                .isSuccess(Boolean.TRUE)
                .dateTime(0l)
                .build();

        dataService.saveMigrationMetadata(metadata);

        metadata.setDateTime(7l);
        metadata.setIsSuccess(Boolean.FALSE);
        metadata.setVersion(8l);

        dataService.updateMigrationMetadata(metadata);

        MigrationMetadata updatedMetadata = dataService.getMigrationMetadata("orders").get();

        assertThat(updatedMetadata.getId(), is(1));
        assertThat(updatedMetadata.getDateTime(), is(7l));
        assertThat(updatedMetadata.getStream(), is("orders"));
        assertThat(updatedMetadata.getIsSuccess(), is(Boolean.FALSE));
        assertThat(updatedMetadata.getVersion(), is(8l));
    }
}

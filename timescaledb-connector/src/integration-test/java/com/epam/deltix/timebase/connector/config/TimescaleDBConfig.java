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
package com.epam.deltix.timebase.connector.config;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.PostgreSQLContainer;

public class TimescaleDBConfig {

    private static final Log LOG = LogFactory.getLog(TimescaleDBConfig.class);

    private static PostgreSQLContainer timescale = new PostgreSQLContainer("timescale/timescaledb:latest-pg12")
            .withDatabaseName("test")
            .withUsername("test")
            .withPassword("test");

    public static void start() {
        timescale.start();
    }

    public static void stop() {
        timescale.stop();
    }

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            LOG.info().append("TimescaleDB started at ").append(timescale.getJdbcUrl()).commit();
            TestPropertyValues.of("spring.datasource.url=" + timescale.getJdbcUrl(),
                    "spring.datasource.username=" + timescale.getUsername(),
                    "spring.datasource.password=" + timescale.getPassword())
                    .applyTo(configurableApplicationContext.getEnvironment());
        }
    }
}

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
package com.epam.deltix.timebase.connector.service;

import com.epam.deltix.timebase.connector.config.ApplicationTestConfig;
import com.epam.deltix.timebase.connector.config.TimescaleDBConfig;
import com.epam.deltix.timebase.connector.config.TimeBaseConfig;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.transaction.annotation.Transactional;

@Transactional
@ActiveProfiles("test")
@ContextConfiguration(initializers = {
        TimescaleDBConfig.Initializer.class,
        TimeBaseConfig.Initializer.class
})
@SpringBootTest(classes = ApplicationTestConfig.class)
public class BaseServiceIntegrationTest {

    @BeforeAll
    public static void init() {
        TimescaleDBConfig.start();
        TimeBaseConfig.start();
    }
}

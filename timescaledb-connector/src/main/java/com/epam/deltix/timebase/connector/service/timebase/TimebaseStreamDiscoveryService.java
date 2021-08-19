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

import com.epam.deltix.gflog.api.*;
import com.epam.deltix.qsrv.hf.tickdb.pub.DXTickDB;
import com.epam.deltix.qsrv.hf.tickdb.pub.DXTickStream;
import com.epam.deltix.timebase.connector.event.NewStreamEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class TimebaseStreamDiscoveryService {

    private static final Log LOG = LogFactory.getLog(TimebaseStreamDiscoveryService.class);

    private final TimebaseConnectionService timebaseConnection;
    private final StreamMetaDataCacheService metaDataCacheService;
    private final ApplicationEventPublisher eventPublisher;
    private final Boolean isAutoDiscoveryEnabled;

    public TimebaseStreamDiscoveryService(TimebaseConnectionService timebaseConnection,
                                          StreamMetaDataCacheService metaDataCacheService,
                                          ApplicationEventPublisher eventPublisher,
                                          @Value("${timebase.autoDiscovery}") Boolean isAutoDiscoveryEnabled) {
        this.timebaseConnection = timebaseConnection;
        this.metaDataCacheService = metaDataCacheService;
        this.eventPublisher = eventPublisher;
        this.isAutoDiscoveryEnabled = isAutoDiscoveryEnabled;
    }

    @Scheduled(fixedDelay = 60000, initialDelay = 120000)
    void discoverStreams() {
        if (!isAutoDiscoveryEnabled) {
            return;
        }

        if (!timebaseConnection.isOpen()) {
            timebaseConnection.init(true);
        }

        DXTickDB connection = timebaseConnection.getConnection();
        DXTickStream[] streams = connection.listStreams();

        for (DXTickStream stream: streams) {
            if (!metaDataCacheService.contains(stream.getName())) {
                LOG.debug().append("New stream detected: ").append(stream.getName()).commit();
                eventPublisher.publishEvent(new NewStreamEvent(stream.getName()));
            }
        }
    }

    public List<String> discover(String wildcard) {
        List<String> result = new ArrayList<>();
        if (!timebaseConnection.isOpen()) {
            timebaseConnection.init(true);
        }
        DXTickDB connection = timebaseConnection.getConnection();
        DXTickStream[] streams = connection.listStreams();

        for (DXTickStream stream: streams) {
            if (stream.getName().matches(wildcard)) {
                result.add(stream.getName());
            }
        }
        return result;
    }
}

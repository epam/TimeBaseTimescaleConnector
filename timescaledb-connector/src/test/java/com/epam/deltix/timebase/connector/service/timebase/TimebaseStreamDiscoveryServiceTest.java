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

import com.epam.deltix.qsrv.hf.tickdb.pub.DXTickDB;
import com.epam.deltix.qsrv.hf.tickdb.pub.DXTickStream;
import com.epam.deltix.timebase.connector.util.DiscoveryUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationEventPublisher;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TimebaseStreamDiscoveryServiceTest {

    @Mock
    private TimebaseConnectionService timebaseConnectionService;
    @Mock
    private StreamMetaDataCacheService metaDataCacheService;
    @Mock
    private ApplicationEventPublisher eventPublisher;

    @InjectMocks
    private TimebaseStreamDiscoveryService discoveryService;

    @Test
    public void testWildcardDiscovery() {
        when(timebaseConnectionService.isOpen()).thenReturn(Boolean.TRUE);
        DXTickDB connectionMock = mock(DXTickDB.class);
        when(timebaseConnectionService.getConnection()).thenReturn(connectionMock);
        DXTickStream stream1Mock = mock(DXTickStream.class);
        when(stream1Mock.getName()).thenReturn("coinbase-binance");
        DXTickStream stream2Mock = mock(DXTickStream.class);
        when(stream2Mock.getName()).thenReturn("binance-orders");
        DXTickStream stream3Mock = mock(DXTickStream.class);
        when(stream3Mock.getName()).thenReturn("binance-accounts");

        DXTickStream[] mockArray = new DXTickStream[]{stream1Mock, stream2Mock, stream3Mock};
        when(connectionMock.listStreams()).thenReturn(mockArray);

        List<String> actualResults = discoveryService.discover(DiscoveryUtils.generateRegExp("binance*"));
        assertThat(actualResults.size(), is(2));
    }
}

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
import com.epam.deltix.qsrv.hf.tickdb.pub.TickDBFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class TimebaseConnectionService {

    private static final Log LOG = LogFactory.getLog(TimebaseConnectionService.class);

    private final DXTickDB timebase;
    private AtomicBoolean isOpen = new AtomicBoolean();
    private final ReentrantLock lock = new ReentrantLock();

    public TimebaseConnectionService(String timebaseUrl) {
        this.timebase = TickDBFactory.createFromUrl(timebaseUrl);
    }

    public DXTickStream getStream(String streamName) {
        lock.lock();
        try {
            if (!isOpen.get()) {
                init(Boolean.TRUE);
            }
        } catch (Exception ex) {
            LOG.error().append("Could not open timebase connection").append(ex).commit();
            isOpen.set(Boolean.FALSE);
        } finally {
            lock.unlock();
        }

        LOG.debug().append("Try to open stream: ").append(streamName).commit();
        return timebase.getStream(streamName);
    }

    public boolean isOpen() {
        return timebase.isOpen();
    }

    protected void init(boolean readOnly) {
        LOG.info().append("Opening timebase connection. ReadOnly: ").append(readOnly).commit();
        timebase.open(readOnly);
        isOpen.set(Boolean.TRUE);
    }

    protected DXTickDB getConnection() {
        return timebase;
    }
}

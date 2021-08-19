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
package com.epam.deltix.timebase.connector.model.system;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class StreamMetaData {
    private AtomicInteger failedAttempts;
    private AtomicReference<Status> status;
    private Instant updateTime;

    public StreamMetaData(Status status) {
        this.status = new AtomicReference<>(status);
        this.updateTime = Instant.now();
        this.failedAttempts = new AtomicInteger();
    }

    public void updateStatus(Status status) {
        this.status.getAndSet(status);
        this.updateTime = Instant.now();
        if (status.equals(Status.FAILED)) {
            failedAttempts.incrementAndGet();
        }
    }

    public Integer getFailedAttempts() {
        return failedAttempts.get();
    }

    public enum Status {
        RUNNING, FAILED
    }
}

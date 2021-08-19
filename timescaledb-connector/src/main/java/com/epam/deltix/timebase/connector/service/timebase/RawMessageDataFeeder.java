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
import com.epam.deltix.qsrv.hf.pub.ChannelQualityOfService;
import com.epam.deltix.qsrv.hf.pub.RawMessage;
import com.epam.deltix.qsrv.hf.pub.TypeLoader;
import com.epam.deltix.qsrv.hf.pub.TypeLoaderImpl;
import com.epam.deltix.qsrv.hf.pub.codec.CodecFactory;
import com.epam.deltix.qsrv.hf.pub.codec.FixedExternalDecoder;
import com.epam.deltix.qsrv.hf.tickdb.pub.DXTickStream;
import com.epam.deltix.qsrv.hf.tickdb.pub.SelectionOptions;
import com.epam.deltix.qsrv.hf.tickdb.pub.TickCursor;
import com.epam.deltix.timebase.connector.service.DataFeeder;
import com.epam.deltix.timebase.connector.service.MigrationService;
import com.epam.deltix.timebase.messages.InstrumentMessage;
import com.epam.deltix.timebase.messages.schema.SchemaChangeMessage;
import com.epam.deltix.util.memory.MemoryDataInput;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

//TODO check exception handling on fetching
public class RawMessageDataFeeder implements DataFeeder<RawMessage> {

    private static final Log LOG = LogFactory.getLog(RawMessageDataFeeder.class);

    private static final Integer DEFAULT_THRESHOLD = 5_000;

    protected static final TypeLoader TYPE_LOADER = TypeLoaderImpl.DEFAULT_INSTANCE;

    private final TransferQueue<RawMessage> queue = new LinkedTransferQueue<>();
    private final AtomicBoolean isFetchingStart = new AtomicBoolean();
    private final AtomicLong consumedMessages = new AtomicLong();
    private final ReentrantLock lock = new ReentrantLock();

    private final MemoryDataInput input = new MemoryDataInput();

    private final DXTickStream stream;
    private final Integer batchSize;
    private final long recoveryTime;
    private final Executor executor;
    private final MigrationService<SchemaChangeMessage> migrationService;

    private TickCursor cursor;

    public RawMessageDataFeeder(DXTickStream stream, Integer batchSize, long recoveryTime, Executor executor,
                                MigrationService<SchemaChangeMessage> migrationService) {
        this.stream = stream;
        this.batchSize = batchSize;
        this.recoveryTime = recoveryTime;
        this.executor = executor;
        this.migrationService = migrationService;
    }

    public RawMessageDataFeeder(DXTickStream stream, MigrationService<SchemaChangeMessage> migrationService) {
        this(stream, DEFAULT_THRESHOLD, 0l, Executors.newSingleThreadExecutor(), migrationService);
    }

    public RawMessageDataFeeder(DXTickStream stream, long recoveryTime, MigrationService<SchemaChangeMessage> migrationService) {
        this(stream, DEFAULT_THRESHOLD, recoveryTime, Executors.newSingleThreadExecutor(), migrationService);
    }

    public List<RawMessage> fetchData(int size) {
        lock.lock();
        try {
            if (!isFetchingStart.get()) {
                executor.execute(() -> {
                    try {
                        fetchDataStream();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        } finally {
            lock.unlock();
        }

        if (queue.isEmpty()) {
            return Collections.emptyList();
        }

        List<RawMessage> result = new ArrayList<>();
        queue.drainTo(result, size);

        return result;
    }

    private void fetchDataStream() throws InterruptedException {
        if (stream == null) {
            LOG.error()
                    .append("Stream could not be NULL.")
                    .commit();
            return;
        }

        try {
            SelectionOptions selectionOptions = new SelectionOptions(true, true, ChannelQualityOfService.MAX_THROUGHPUT);
            selectionOptions.versionTracking = true;
            cursor = stream.select(
                    recoveryTime,
                    selectionOptions,
                    null,
                    (CharSequence[]) null
            );

            while (cursor.next()) {
                InstrumentMessage message = cursor.getMessage();
                InstrumentMessage messageClone = message.clone();

                RawMessage msg = (RawMessage) messageClone;

                if (msg != null) {
                    // handle migration messages
                    if ("@SYSTEM".equals(msg.getSymbol())) {
                        //TODO remove workaround when when subscription feature will be released
                        if (msg.type != null && SchemaChangeMessage.class.getName().equals(msg.type.getName())) {
                            SchemaChangeMessage changeMessage = boundDecode(msg);
                            migrationService.apply(changeMessage, stream.getName());
                        }
                        continue;
                    }

                    if (consumedMessages.incrementAndGet() % batchSize == 0) {
                        queue.transfer(msg);
                    } else {
                        queue.add(msg);
                    }
                } else {
                    LOG.warn().append("Null element occurred in stream: ").append(stream.getName()).commit();
                }
            }
        } finally {
            close();
        }
    }

    @Override
    public void close() {
        if (cursor != null && !cursor.isClosed()) {
            LOG.info().append("Closing cursor for stream: ").append(stream.getName()).commit();
            cursor.close();
        }
    }

    public SchemaChangeMessage boundDecode(RawMessage msg) {
        input.reset(0);
        input.setBytes(msg.data);
        FixedExternalDecoder bdec = CodecFactory.COMPILED.createFixedExternalDecoder(TYPE_LOADER, msg.type);
        SchemaChangeMessage changeMessage = new SchemaChangeMessage();
        bdec.setStaticFields(changeMessage);
        bdec.decode(input, changeMessage);
        return changeMessage;
    }
}

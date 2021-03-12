package deltix.timebase.connector.service.timebase;

import deltix.gflog.Log;
import deltix.gflog.LogFactory;
import deltix.qsrv.hf.tickdb.pub.DXTickDB;
import deltix.qsrv.hf.tickdb.pub.DXTickStream;
import deltix.qsrv.hf.tickdb.pub.TickDBFactory;

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

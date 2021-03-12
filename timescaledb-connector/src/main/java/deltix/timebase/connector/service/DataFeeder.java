package deltix.timebase.connector.service;

import deltix.timebase.messages.InstrumentMessage;

import java.util.List;

public interface DataFeeder<T extends InstrumentMessage> {

    List<T> fetchData(int batchSize);

    void close();
}

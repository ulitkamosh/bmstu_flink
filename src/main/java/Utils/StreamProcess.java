package Utils;

import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.IOException;

public interface StreamProcess {
    void process(DataStream<InputData> inputStream) throws IOException;

}

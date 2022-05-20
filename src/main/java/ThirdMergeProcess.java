import Utils.FolderChecker;
import Utils.InputData;
import Utils.StreamProcess;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

import java.io.IOException;

public class ThirdMergeProcess implements StreamProcess {

    // Prepare the output directory (sink). It will store the output of the process action on the incoming stream.
    private static final String outputDir = "data/sink_summary";

    // This method receives a stream and executes split operation
    public void process(DataStream<InputData> inputStream) throws IOException {
        FolderChecker.checkFolder(outputDir);

        // Разделяем поток
        Tuple2<DataStream<InputData>, DataStream<InputData>> getSplits =
                SecondSplitProcess.
                        splitStreamProcess(inputStream);

        // Соединяем два потока в один
        mergeStreamProcess(
            FirstBasicProcess.countingStream(getSplits.f0),
            FirstBasicProcess.countingStream(getSplits.f1)
        );


//        JoinedStreams<Tuple2<String, Integer>,Tuple2<String, Integer>> joinedStream =
//                FirstBasicProcess.countingStream(getSplits.f0)
//                        .join(FirstBasicProcess.countingStream(getSplits.f1));

    }

    // This method implements a merge stream operation. The inputs are two types of streams, which converges into a third type.
    public static DataStream<Tuple3<String, String, Integer>> mergeStreamProcess(DataStream<Tuple2<String, Integer>> inputStream_1,
                       DataStream<Tuple2<String, Integer>> inputStream_2) {

        // The returned stream definition includes both streams data type
        ConnectedStreams<Tuple2<String, Integer>, Tuple2<String, Integer>> mergedStream
                = inputStream_1
                .connect(inputStream_2);


        DataStream<Tuple3<String, String, Integer>> combinedStream
                = mergedStream.map(new CoMapFunction<
                Tuple2<String, Integer>, //Stream 1
                Tuple2<String, Integer>, //Stream 2
                Tuple3<String, String, Integer> //Output
                >() {

            @Override
            public Tuple3<String, String, Integer>  //Process Stream 1
            map1(Tuple2<String, Integer> item) throws Exception {
                return new Tuple3<String, String, Integer>
                        ("Foreign merged stream :", item.f0, item.f1);
            }

            @Override
            public Tuple3<String, String, Integer> map2(Tuple2<String, Integer> item) throws Exception {
                return new Tuple3<String, String, Integer>
                        ("Ally merged stream:", item.f0, item.f1);
            }
        });
        combinedStream.print();

        /************************* Sink implementation *****************/

        final StreamingFileSink<Tuple3<String, String, Integer>> countSink
                = StreamingFileSink
                .forRowFormat(new Path(outputDir),
                        new SimpleStringEncoder<Tuple3<String, String, Integer>>
                                ("UTF-8"))
                .build();

        // Add the sink file stream to the DataStream; with that, the inputCountSummary will be written into the FileSink path
        combinedStream.addSink(countSink);

        return  combinedStream;
    }
}

import Utils.AdditionalInfo;
import Utils.FolderChecker;
import Utils.InputData;
import Utils.StreamProcess;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.experimental.CollectSink;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.time.Instant;
import java.util.*;

public class FirstBasicProcess implements StreamProcess {

    // Prepare the output directory (sink). It will store the output of the process action on the incoming stream.
    private static final String outputDir = "data/sink_summary";
//    private Long msgCount = 0L;
//    static final AdditionalInfo additionalInfo = new AdditionalInfo();
    static final AdditionalInfo additionalInfo = new AdditionalInfo();



    @Override
    public void process(DataStream<InputData> inputStream) throws IOException {

        FolderChecker.checkFolder(outputDir);

        countingStream(inputStream);
    }

    public static DataStream<Tuple2<String, Integer>> countingStream(DataStream<InputData> inputStream) {
        /************************* Group By Key implementation *****************/

        // Convert each record to a Tuple with name and score
        DataStream<Tuple2<String, Integer>> countriesCount
                = inputStream
                .process((new ProcessFunction<InputData, InputData>() {
                        @Override
                        public void processElement(InputData inputData, Context context, Collector<InputData> collector) throws Exception {
                            additionalInfo.registerString(inputData);
                            collector.collect(inputData);
//                            System.out.println(Date.from(Instant.now()).getTime() - inputData.getTimeCreated());
                            System.out.printf("%s %s %s%n", "\u001B[35m", additionalInfo, "\u001B[0m");

                        }
                    })
                )
                .map((MapFunction<InputData, Tuple2<String, Integer>>) item ->
                        new Tuple2<>(item.getCountryName(), 1)
                ).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)  // returns KeyedStream<T, Tuple> based on the first item ('name' fields)
                .timeWindow(Time.seconds(2)) // return WindowedStream<T, KEY, TimeWindow>
                .reduce((x,y) -> new Tuple2<>(
                        x.f0+"-"+y.f0,
                        x.f1+y.f1
                    )
                );

        //Date.from(Instant.now()).getTime()

        countriesCount.print();

        /************************* Sink implementation *****************/

        final StreamingFileSink<Tuple2<String, Integer>> countSink
                = StreamingFileSink
                .forRowFormat(new Path(outputDir),
                        new SimpleStringEncoder<Tuple2<String, Integer>>
                                ("UTF-8"))
                .build();

        // Add the sink file stream to the DataStream; with that, the inputCountSummary will be written into the FileSink path
        countriesCount.addSink(countSink);

        return countriesCount;
    }

    public static void countingStream3(DataStream<Tuple3<String, String, Integer>>  inputStream) {
        /************************* Group By Key implementation *****************/

        // Convert each record to a Tuple with name and score
        DataStream<Tuple2<String, Integer>> countriesCount
                = inputStream
                .map((MapFunction<Tuple3<String, String, Integer>, Tuple2<String, Integer>>) item ->
                        new Tuple2<>(item.f0 + " " + item.f1, item.f2)
                ).returns(Types.TUPLE(Types.STRING, Types.INT));

        countriesCount.print();

        /************************* Sink implementation *****************/

        final StreamingFileSink<Tuple2<String, Integer>> countSink
                = StreamingFileSink
                .forRowFormat(new Path(outputDir),
                        new SimpleStringEncoder<Tuple2<String, Integer>>
                                ("UTF-8"))
                .build();

        // Add the sink file stream to the DataStream; with that, the inputCountSummary will be written into the FileSink path
        countriesCount.addSink(countSink);

    }
}

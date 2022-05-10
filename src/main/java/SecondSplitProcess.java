import Utils.FolderChecker;
import Utils.InputData;
import Utils.StreamProcess;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.IOException;

public class SecondSplitProcess implements StreamProcess {

    // Prepare the output directory (sink). It will store the output of the process action on the incoming stream.
    private static final String outputDir = "data/sink_summary";

    // This method receives a stream and executes split operation
    public void process(DataStream<InputData> inputStream) throws IOException {

        FolderChecker.checkFolder(outputDir);

        // Вывод в консоль элементов
//        inputStream
//                .map(elem -> {
//                    String msg = "Print item : " + elem.CountryName;
//                    System.out.println(String.format("%s %s %s", "\u001B[33m", msg, "\u001B[0m"));
//                    return (elem);
//
//                }).timeWindowAll(Time.seconds(windowInterval));

//        SingleOutputStreamOperator<Integer> map = inputStream
//                // Define a counter record for each input item
//                .map(item
//                        -> new Integer(1))
//                .returns(Types.INT)
//                // Set window by time
//                .timeWindowAll(Time.seconds(windowTime))
//
//                // Accumulate the number of records in each time window's interval
//                .sum(0)
//
//                // Execute print method, since there is only one item in the stream the method will be executed once
//                .map(new MapFunction<Integer, Integer>() {
//                    @Override
//                    public Integer map(Integer recordCount) throws Exception {
//                        String msg = "InputData objects count in the last " + windowTime + " seconds " + recordCount;
//                        System.out.println(String.format("%s %s %s", "\u001B[32m", msg, "\u001B[0m"));
//                        return recordCount;
//                    }
//                });

        Tuple2<DataStream<InputData>, DataStream<InputData>> getSplits = splitStreamProcess(inputStream);
        FirstBasicProcess.countingStream(getSplits.f0);
        FirstBasicProcess.countingStream(getSplits.f1);

    }


    public static Tuple2<DataStream<InputData>, DataStream<InputData>> splitStreamProcess(DataStream<InputData> inputStream){

        final OutputTag<InputData> outputTag = new OutputTag<InputData>("side-output") {};
        final OutputTag<InputData> outputTag2 = new OutputTag<InputData>("side-output_2") {};

        SingleOutputStreamOperator<InputData> mainDataStream = inputStream
                .process(new ProcessFunction<InputData, InputData>() {
                    @Override
                    public void processElement(InputData inputData, Context context, Collector<InputData> collector) throws Exception {
                        // emit data to regular output
                        collector.collect(inputData);

                        // emit data to side output
                        context.output(outputTag, inputData);
                        context.output(outputTag2, inputData);

                    }
                });
        DataStream<InputData> sideOutputStream = mainDataStream.getSideOutput(outputTag);
        DataStream<InputData> sideOutputStream2 = mainDataStream.getSideOutput(outputTag2);

        int windowInterval = 5;
        // Вывод в консоль элементов
        sideOutputStream
                .map(elem -> {
                    String msg = "sideOutputStream item : " + elem.getCountryName();
                    System.out.printf("%s %s %s%n", "\u001B[33m", msg, "\u001B[0m");
                    return (elem);

                }).timeWindowAll(Time.seconds(windowInterval));
        // Вывод в консоль элементов
        sideOutputStream2
                .map(elem -> {
                    String msg = "sideOutputStream2 item : " + elem.getCountryName();
                    System.out.printf("%s %s %s%n", "\u001B[33m", msg, "\u001B[0m");
                    return (elem);

                }).timeWindowAll(Time.seconds(windowInterval));

        return new Tuple2<>(sideOutputStream, sideOutputStream2);
    }
}

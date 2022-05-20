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

        Tuple2<DataStream<InputData>, DataStream<InputData>> getSplits = splitStreamProcess(inputStream);
        FirstBasicProcess.countingStream(getSplits.f0);
        FirstBasicProcess.countingStream(getSplits.f1);

    }


    public static Tuple2<DataStream<InputData>, DataStream<InputData>> splitStreamProcess(DataStream<InputData> inputStream){

        final OutputTag<InputData> outputTag = new OutputTag<InputData>("Foreign") {};
        final OutputTag<InputData> outputTag2 = new OutputTag<InputData>("Ally") {};

        SingleOutputStreamOperator<InputData> mainDataStream = inputStream
                .process(new ProcessFunction<InputData, InputData>() {
                    @Override
                    public void processElement(InputData inputData, Context context, Collector<InputData> collector) throws Exception {

                        // Разделяем поток в зависимости от названия страны
                        // в outputTag проходят только USA и Germany
                        // в outputTag2 проходят только Ukraine и Russia
                        switch (inputData.getCountryName()){
                            case "USA":
                            case "Germany":
                                context.output(outputTag, inputData);
                                break;
                            case "Ukraine":
                            case "Russia":
                                context.output(outputTag2, inputData);
                                break;
                        }

                    }
                });
        DataStream<InputData> sideOutputStream = mainDataStream.getSideOutput(outputTag);
        DataStream<InputData> sideOutputStream2 = mainDataStream.getSideOutput(outputTag2);

        int windowInterval = 5;
        // Вывод в консоль элементов
        sideOutputStream
                .map(elem -> {
                    String msg = "Foreign item : " + elem.getCountryName();
                    System.out.printf("%s %s %s%n", "\u001B[33m", msg, "\u001B[0m");
                    return (elem);

                }).timeWindowAll(Time.seconds(windowInterval));
        // Вывод в консоль элементов
        sideOutputStream2
                .map(elem -> {
                    String msg = "Ally item : " + elem.getCountryName();
                    System.out.printf("%s %s %s%n", "\u001B[33m", msg, "\u001B[0m");
                    return (elem);

                }).timeWindowAll(Time.seconds(windowInterval));

        return new Tuple2<>(sideOutputStream, sideOutputStream2);
    }
}

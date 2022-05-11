import Utils.GenerateSocketInput;
import Utils.InputData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Scanner;

public class StreamProcessApplication {

    private static final String HOSTNAME = "localhost"; // серверсокет
    private static final int HOSTPORT = 9999; // серверсокет

    public static void main(String[] args) {

        try{
            // Setup a Flink streaming execution environment
            Configuration config = new Configuration();
            config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
            StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);

            streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

            // Keep the ordering of records, therefore use only one thread to process the messages (to avoid changing the sequence of printing)
            streamEnv.setParallelism(1);

            // The default parallel is based on the number of CPU cores
            System.out.println("\nTotal Parallel Task Slots : " + streamEnv.getParallelism());

            //Create a DataStream based on the directory
            DataStream<String> lines = streamEnv.socketTextStream(HOSTNAME, HOSTPORT);

            DataStream<InputData> dataStream = lines
                    .map((MapFunction<String, InputData>) inputStr -> {
                        System.out.println("--- Received Record : " + inputStr);
                        return InputData.getDataObject(inputStr);
                    }
            );

            Utils.StreamProcess streamOperation = userSelection();
            while (streamOperation==null)
                streamOperation = userSelection();

            streamOperation.process(dataStream);

            //Start the File Stream generator on a separate thread
            startGeneratingStream(10);

            // execute the streaming pipeline
            streamEnv.execute("Flink Streaming process");

        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    private static Utils.StreamProcess userSelection(){
        System.out.println("Please select the stream processing function:\n" +
                "* '1' for basic stream operation\n" +
                "* '2' for split stream operation\n" +
                "* '3' for split and merge stream operations\n" +
                "* 'e' to exit");

        Scanner scanner = new Scanner(System.in);
        String str = scanner.next();

        // Create a stream processing object based on the user's selection.
        if(str.startsWith("1")) {
            return new FirstBasicProcess();
        }
        else if(str.startsWith("2")) {
            return new SecondSplitProcess();
        }
        else if(str.startsWith("3")) {
            return new ThirdMergeProcess();
        }
        else if(str.startsWith("e")) {
            System.exit(0);
        }
        else {
            System.out.println("Unknown input "+str);
        }

        return null;
    }

    private static void startGeneratingStream(Integer numOfFiles){
        System.out.println("Starting File Data Generator...");
        Thread genThread = new Thread(new GenerateSocketInput(numOfFiles, HOSTNAME, HOSTPORT));
        genThread.start();
    }
}

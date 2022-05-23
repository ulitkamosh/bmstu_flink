//import Utils.FolderChecker;
//import Utils.GenerateSocketInput;
//import Utils.InputData;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.configuration.ConfigConstants;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.junit.jupiter.api.AfterEach;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//
//import java.io.IOException;
//import java.io.PrintWriter;
//import java.net.ServerSocket;
//import java.net.Socket;
//import java.util.Random;
//
//import static org.junit.jupiter.api.Assertions.*;
//
//class FirstBasicProcessTest {
//
//    private static final String HOSTNAME = "localhost"; // серверсокет
//    private static final int HOSTPORT = 9999; // серверсокет
//    private static final String outputDir = "data/sink_summary";
//
//    DataStream<InputData> dataStream;
//    StreamExecutionEnvironment streamEnv;
//    PrintWriter out;
//    ServerSocket server;
//
//    @BeforeEach
//    void setUp() {
//        try{
//            // Setup a Flink streaming execution environment
//            Configuration config = new Configuration();
//            config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
//            streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
//
//            streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
//
//            // Keep the ordering of records, therefore use only one thread to process the messages (to avoid changing the sequence of printing)
//            streamEnv.setParallelism(1);
//
//            // The default parallel is based on the number of CPU cores
//            System.out.println("\nTotal Parallel Task Slots : " + streamEnv.getParallelism());
//
//            //Create a DataStream based on the directory
//            DataStream<String> lines = streamEnv.socketTextStream(HOSTNAME, HOSTPORT);
//
//            dataStream = lines
//                    .map((MapFunction<String, InputData>) inputStr -> {
//                                System.out.println("--- Received Record : " + inputStr);
//                                return InputData.getDataObject(inputStr);
//                            }
//                    );
//
//        }
//        catch(Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    @AfterEach
//    void tearDown() throws IOException {
//        out.close();
//        server.close();
//    }
//
//    @Test
//    void countingStream() throws Exception {
//        FolderChecker.checkFolder(outputDir);
//
//        FirstBasicProcess.countingStream(dataStream);
//
//        Thread genThread = new Thread(new serverSocket());
//        genThread.start();
//        // execute the streaming pipeline
//        streamEnv.execute("Flink Streaming process");
//
//    }
//
//    public class serverSocket implements Runnable{
//
//        @Override
//        public void run() {
//            try {
//                server = new ServerSocket(HOSTPORT);
//                Socket clientSocket = server.accept();
//                out = new PrintWriter(clientSocket.getOutputStream(), true);
//
//                Random random = new Random();
//
//                out.println("[1, USA]");
//                Thread.sleep(random.nextInt(1000),25);
//                out.println("[2, USA]");
//                Thread.sleep(random.nextInt(1000),25);
//                out.println("[3, USA]");
//                Thread.sleep(random.nextInt(1000),50000);
//                out.close();
//
//            } catch (IOException | InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//
//}
package Utils;

import com.opencsv.CSVWriter;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class GenerateSocketInput  implements Runnable {
    private static ServerSocket server; // серверсокет
    private static Socket clientSocket;
    private static PrintWriter out;

    private static String HOSTNAME;
    private static int HOSTPORT;

    public GenerateSocketInput(Integer cycles, String hostName, int hostPort){
        numOfInputs = cycles;
        HOSTNAME = hostName;
        HOSTPORT = hostPort;
    }

    // The default number of files to generate
    private  static Integer numOfInputs = 10;

    public void run() {

        try {
            server = new ServerSocket(HOSTPORT);
            clientSocket = server.accept();
            out = new PrintWriter(clientSocket.getOutputStream(), true);

            // Define a list of names
            List<String> names = new ArrayList<String>(
                    Arrays.asList("Russia", "Ukraine", "USA", "Germany"));

            //Define list of application entities

            // Use a random object
            Random random = new Random();

            // Generate sample data records, two per each file
            for(int i=0; i < numOfInputs; i++) {

                // Create a text array, with the content
                List<String> FormatText = new ArrayList<String>(
                        Arrays.asList(
                                String.valueOf(i),
                                names.get(random.nextInt(names.size()))

                                //System.lineSeparator(), // a new line

                                //String.valueOf(++i),
                                //names.get(random.nextInt(names.size()))
                        )
                ) ;

                String msg = Arrays.toString(FormatText.toArray());
                System.out.println(String.format("%s %s %s", "\u001B[34m", "Creating item: " + msg, "\u001B[0m"));
                out.println(msg);

                // Sleep for a random time the next cycle.
                Thread.sleep(random.nextInt(1000) + 0,25);
            }

            // Once one cycle is finished, the user can select to start another one or quit.
            System.out.println("Finished one cycle of input generator. Waiting for instructions:\n - 'c' to start another cycle\n - 'e' to exit");

            Scanner scanner = new Scanner(System.in);
            String str = scanner.next();

            // Decide whether to run another cycle based on the user's choice.
            if(str.startsWith("c")) {
                System.out.println("Starting another cycle");
                run();
            }if(str.startsWith("e")) {
                System.exit(0);
            }else {
                System.out.println("Unknown input "+str);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}

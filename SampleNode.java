import java.util.Scanner;

/**
 * A sample node which extends the connection manager.
 * A shell is provided for testing the connection manager.
 * @author Shenqi Zhang
 *
 */
public class SampleNode extends ConnectionManager {
    /**
     * Constructs a sample node with specified name and log name.
     * @param name name of this sample node.
     * @param logName name of the log file
     */
    public SampleNode(String name, String logName) {
        super(name, true, true, logName);
    }
    
    /**
     * Handles the request from the source. Here we just echo the request.
     * @param source source of the request in the distributed system
     * @param request request
     */
    @Override
    protected void handleRequest(String source, String request) {
        sendResponse(source, request);
    }
    
    /**
     * Handles the message from the source. Here we do nothing.
     * @param source the source in the distributed system
     * @param message message
     */
    @Override
    protected void handleMessage(String source, String message) {}
    
    /**
     * A shell which allow users to test the connection manager.
     * @param args arguments
     */
    public static void main(String[] args) {
        SampleNode node = new SampleNode(args[0], args.length >= 2 ? args[1] : null);
        
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println();
            System.out.println("1: send a request");
            System.out.println("2: send a message");
            System.out.println("X: kill");
            System.out.println("Please input your operation:");
            String operation = scanner.next();
            
            if (operation.toLowerCase().equals("x")) {
                scanner.close();
                System.exit(0);
            }
            
            if (!operation.matches("[1-2]")) {
                System.out.println("Error: Invalid operation!");
                continue;
            }
            
            System.out.println("Please input the destination:");
            String destination = scanner.next();
            
            if (operation.equals("1")) {
                System.out.println("Please input your request:");
                String request = scanner.next();
                node.sendRequest(destination, request);
            } else {
                System.out.println("Please input your message:");
                String message = scanner.next();
                node.sendMessage(destination, message);
            }
        }
    }
}

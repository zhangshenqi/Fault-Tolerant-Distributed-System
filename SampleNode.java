import java.util.Scanner;

public class SampleNode extends ConnectionManager {
    public SampleNode(String name, String logName) {
        super(name, true, true, logName);
    }
    
    @Override
    protected void handleRequest(String source, String request) {
        sendResponse(source, request);
    }
    
    @Override
    protected void handleMessage(String source, String message) {}
    
    /**
     * A shell which sends user-specified requests and receives responses.
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

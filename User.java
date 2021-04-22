import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;

/**
 * A basic user which sends requests to replicas.
 * @author Shenqi Zhang
 *
 */
public class User extends ConnectionManager {
    /**
     * Replica manager.
     */
    protected String replicaManager;
    
    /**
     * Constructs a user.
     * @param name the name of this node in the distributed system
     */
    public User(String name) {
        this(name, null);
    }
    
    /**
     * Constructs a user.
     * @param name the name of this node in the distributed system
     * @param logName the name of the log file; if null, log will be written in stdout
     */
    public User(String name, String logName) {
        super(name, false, false, logName);
        Map<String, String> parameters = getParameters("user.conf");
        this.replicaManager = parameters.get("replica_manager");
        
        printParameters();
    }
    
    /**
     * Handles the request from the source.
     * @param source source of the request in the distributed system
     * @param request request
     */
    @Override
    protected void handleRequest(String source, String request) {}
    
    /**
     * Handles the message from the source.
     * @param source the source in the distributed system
     * @param message message
     */
    @Override
    protected void handleMessage(String source, String message) {}
    
    /**
     * Sends request to replicas.
     * @param request request
     * @return response
     */
    protected String sendRequestToReplicas(String request) {
        String membership = sendRequest(replicaManager, "Membership");
        if (membership.length() == 0) {
            return "Error: No server is available!";
        }
        
        Map<String, String> responses = sendRequestToGroup(Arrays.asList(membership.split(",")), request);
        for (String response : responses.values()) {
            if (response != null) {
                return response;
            }
        }
        
        return "Error: No server is available!";
    }
    
    /**
     * Tests the specified user node.
     * @param node the specified user node
     */
    protected static void test(User node) {
        if (System.getenv("ENABLE_AUTO_TEST") == null) {
            manualTest(node);
        } else {
            autoTest(node);
        }
    }
    
    /**
     * Tests the specified user node manually.
     * @param node the specified user node
     */
    protected static void manualTest(User node) {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println();
            System.out.println("1: get");
            System.out.println("2: increment");
            System.out.println("3: decrement");
            System.out.println("X: kill");
            System.out.println("Please input your operation:");
            String operation = scanner.next();
            
            if (operation.toLowerCase().equals("x")) {
                scanner.close();
                System.exit(0);
            }
            
            if (!operation.matches("[1-3]")) {
                System.out.println("Error: Invalid operation!");
                continue;
            }
            
            System.out.println("Please input key:");
            String key = scanner.next();
            if (key.length() == 0) {
                System.out.println("Error: Key is empty!");
                continue;
            }
            
            StringBuilder sb = new StringBuilder();
            if (operation.equals("1")) {
                sb.append("Get|");
            } else if (operation.equals("2")) {
                sb.append("Increment|");
            } else {
                sb.append("Decrement|");
            }
            sb.append(key).append(',').append(System.currentTimeMillis());
            String request = sb.toString();
            String response = node.sendRequestToReplicas(request);
            System.out.println(response);
        }
    }
    
    /**
     * Tests the specified user node automatically.
     * @param node the specified user node
     */
    protected static void autoTest(User node) {
        Random random = new Random();
        while (true) {
            try {
                Thread.sleep(random.nextInt(10) + 1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            StringBuilder sb = new StringBuilder();
            int operation = random.nextInt(3) + 1;
            switch (operation) {
            case 1:
                sb.append("Get|");
                break;
            case 2:
                sb.append("Increment|");
                break;
            case 3:
                sb.append("Decrement|");
                break;
            default:
                System.out.println("Error: Invalid operation!");
                continue;
            }
            sb.append((char) (random.nextInt(3) + 'A')).append(',').append(System.currentTimeMillis());
            String request = sb.toString();
            String response = node.sendRequestToReplicas(request);
            System.out.println(response);
        }
    }
    
    /**
     * Prints the parameters.
     */
    private void printParameters() {
        printLog("replica_manager = " + this.replicaManager);
    }
    
    /**
     * Launches a user and tests it.
     * @param args arguments
     */
    public static void main(String[] args) {
        User node = new User(args[0], args.length >= 2 ? args[1] : null);
        test(node);
    }
}

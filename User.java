import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;
import java.util.Scanner;

public abstract class User extends ConnectionManager {
    protected String replicaManager;
    
    public User(String name) {
        this(name, null);
    }
    
    public User(String name, String connectionManagerLogName) {
        super(name, false, false, connectionManagerLogName);
        RandomAccessFile file = null;
        try {
            file = new RandomAccessFile("user.conf", "r");
            replicaManager = file.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                file.close();
            }  catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    protected void handleRequest(String source, String request) {}

    @Override
    protected void handleMessage(String source, String message) {}
    
    protected abstract String sendRequestToReplicas(String request);
    
    protected static void test(User node) {
        String testMode = System.getenv("USER_TEST_MODE");
        if (testMode != null && testMode.equals("AUTO")) {
            autoTest(node);
        } else {
            manualTest(node);
        }
    }

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
                sb.append("Get,");
            } else if (operation.equals("2")) {
                sb.append("Increment,");
            } else {
                sb.append("Decrement,");
            }
            sb.append(key).append(',').append(System.currentTimeMillis());
            String request = sb.toString();
            String response = node.sendRequestToReplicas(request);
            System.out.println(response);
        }
    }
    
    protected static void autoTest(User node) {
        Random random = new Random();
        while (true) {
            StringBuilder sb = new StringBuilder();
            int operation = random.nextInt(3) + 1;
            switch (operation) {
            case 1:
                sb.append("Get,");
                break;
            case 2:
                sb.append("Increment,");
                break;
            case 3:
                sb.append("Decrement,");
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
}

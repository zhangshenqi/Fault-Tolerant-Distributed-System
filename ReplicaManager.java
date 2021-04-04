import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Scanner;
import java.util.Set;

public class ReplicaManager extends FaultDetector {
    private Set<String> replicas;
    private LinkedHashSet<String> membership;
    
    public ReplicaManager(String name) {
        this(name, 1000, 3, null);
    }
    
    public ReplicaManager(String name, String connectionManagerLogName) {
        this(name, 1000, 3, connectionManagerLogName);
    }
    
    public ReplicaManager(String name, int heartbeatInterval, int heartbeatTolerance) {
        this(name, heartbeatInterval, heartbeatTolerance, null);
    }
    
    /**
     * Constructs a fault detector with specified name and connection manager log name.
     * @param name name of this sample node.
     * @param logName name of the connection manager log file
     */
    public ReplicaManager(String name, int heartbeatInterval, int heartbeatTolerance, String connectionManagerLogName) {
        super(name, heartbeatInterval, heartbeatTolerance, connectionManagerLogName);
        replicas = new HashSet<String>();
        RandomAccessFile file = null;
        try {
            file = new RandomAccessFile("replica_manager.conf", "r");
            String line = file.readLine();
            for (String replica : line.split(",")) {
                replicas.add(replica);
            }
        }  catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                file.close();
            }  catch (IOException e) {
                e.printStackTrace();
            }
        }
        membership = new LinkedHashSet<String>();
    }
    
    @Override
    protected void handleRequest(String source, String request) {
        String[] strs = request.split(",");
        String operation = strs[0];
        
        // Alive,<node>
        if (operation.equals("Alive")) {
            String node = strs[1];
            System.out.println(node + " is alive.");
            sendResponse(source, "ACK");
            if (replicas.contains(node) && membership.add(node)) {
                sendMembershipRequests();
            }
        }
        
        // Dead,<node>
        else if (operation.equals("Dead")) {
            String node = strs[1];
            System.out.println(node + " is dead.");
            sendResponse(source, "ACK");
            if (replicas.contains(node) && membership.remove(node)) {
                sendMembershipRequests();
            }
        }
    }
    
    private String getMembershipRequest() {
        StringBuilder sb = new StringBuilder("Membership");
        for (String member : membership) {
            sb.append(',').append(member);
        }
        return sb.toString();
    }
    
    private void sendMembershipRequests() {
        if (membership.isEmpty()) {
            return;
        }
        
        String request = getMembershipRequest();
        // Still send membership request to all replicas,
        // in case some may just become alive that replica manager hasn't known.
        for (String replica : replicas) {
            sendRequest(replica, request);
        }
    }
    
    public static void main(String[] args) {
        ReplicaManager node = new ReplicaManager(args[0], args.length >= 2 ? args[1] : null);
        
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println();
            System.out.println("1: change heartbeat interval");
            System.out.println("2: change heartbeat tolerance");
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
            
            
        }
    }
}

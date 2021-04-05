import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Scanner;
import java.util.Set;

public class ReplicaManager extends FaultDetector {
    private boolean passive;
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
            String line = file.readLine().trim().toLowerCase();
            if (line.equals("passive")) {
                passive = true;
            } else if (line.equals("active")) {
                passive = false;
            } else {
                throw new IllegalArgumentException("Error: Invalid replica_manager.conf!");
            }
            line = file.readLine();
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
            if (replicas.contains(node)) {
                boolean changed;
                synchronized(membership) {
                    changed = membership.add(node);
                }
                if (changed) {
                    sendMembershipRequests();
                }
            }
        }
        
        // Dead,<node>
        else if (operation.equals("Dead")) {
            String node = strs[1];
            System.out.println(node + " is dead.");
            sendResponse(source, "ACK");
            if (replicas.contains(node)) {
                boolean changed;
                synchronized(membership) {
                    changed = membership.remove(node);
                }
                if (changed) {
                    sendMembershipRequests();
                }
            }
        }
        
        // Membership
        else if (operation.equals("Membership")) {
            sendResponse(source, getMembershipResponse());
        }
    }
    
    private String getMembershipRequest() {
        StringBuilder sb = new StringBuilder("Membership");
        synchronized(membership) {
            for (String member : membership) {
                sb.append(',').append(member);
            }
        }
        return sb.toString();
    }
    
    private void sendMembershipRequests() {
        String request = getMembershipRequest();
        // Still send membership request to all replicas,
        // in case some may just become alive that replica manager hasn't known.
        for (String replica : replicas) {
            sendRequest(replica, request);
        }
    }
    
    private String getMembershipResponse() {
        StringBuilder sb = new StringBuilder();
        synchronized(membership) {
            for (String member : membership) {
                sb.append(member).append(',');
            }
        }
        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }
    
    private void sendCheckpointIntervalRequests(int checkpointInterval) {
        String request = "CheckpointInterval," + checkpointInterval;
        for (String replica : replicas) {
            sendRequest(replica, request);
        }
    }
    
    public static void main(String[] args) {
        ReplicaManager node = new ReplicaManager(args[0], args.length >= 2 ? args[1] : null);
        
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println();
            System.out.println("1: set heartbeat interval");
            System.out.println("2: set heartbeat tolerance");
            if (node.passive) {
                System.out.println("3: set checkpoint interval");
            }
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
            
            if (operation.equals("1")) {
                System.out.println("Please input heartbeat interval:");
                String intervalStr = scanner.next();
                int interval = Integer.valueOf(intervalStr);
                if (interval <= 0) {
                    System.out.println("Error: Invalid heartbeat interval!");
                    continue;
                }
                node.setHeartbeatInterval(interval);
                node.sendRequestsToChildren("HeartbeatInterval," + interval);
            } else if (operation.equals("2")) {
                System.out.println("Please input heartbeat tolerance:");
                String toleranceStr = scanner.next();
                int tolerance = Integer.valueOf(toleranceStr);
                if (tolerance <= 0) {
                    System.out.println("Error: Invalid heartbeat tolerance!");
                    continue;
                }
                node.setHeartbeatTolerance(tolerance);
                node.sendRequestsToChildren("HeartbeatTolerance," + tolerance);
            } else {
                System.out.println("Please input checkpoint interval:");
                String intervalStr = scanner.next();
                int interval = Integer.valueOf(intervalStr);
                if (interval <= 0) {
                    System.out.println("Error: Invalid checkpoint interval!");
                    continue;
                }
                node.sendCheckpointIntervalRequests(interval);
            }
        }
    }
}

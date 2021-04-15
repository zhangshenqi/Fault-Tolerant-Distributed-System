import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ReplicaManager extends FaultDetector {
    private Set<String> replicas;
    private LinkedHashSet<String> membership;
    private final ReentrantReadWriteLock membershipLock;
    
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
        this.replicas = new HashSet<String>();
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
        this.membership = new LinkedHashSet<String>();
        this.membershipLock = new ReentrantReadWriteLock();
    }
    
    @Override
    protected void handleRequest(String source, String request) {
        String[] strs = request.split(",");
        String operation = strs[0];
        
        // Alive,<node>
        if (operation.equals("Alive")) {
            String node = strs[1];
            sendResponse(source, "ACK");
            if (replicas.contains(node)) {
                membershipLock.writeLock().lock();
                try {
                    if (membership.add(node)) {
                        sendRequestToGroup(membership, getMembershipRequest());
                    }
                } finally {
                    membershipLock.writeLock().unlock();
                }
            }
        }
        
        // Dead,<node>
        else if (operation.equals("Dead")) {
            String node = strs[1];
            sendResponse(source, "ACK");
            if (replicas.contains(node)) {
                membershipLock.writeLock().lock();
                try {
                    if (membership.remove(node)) {
                        sendRequestToGroup(membership, getMembershipRequest());
                    }
                } finally {
                    membershipLock.writeLock().unlock();
                }
            }
        }
        
        // Membership
        else if (operation.equals("Membership")) {
            membershipLock.readLock().lock();
            try {
                sendResponse(source, getMembershipResponse());
            } finally {
                membershipLock.readLock().unlock();
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
    
    private String getMembershipResponse() {
        if (membership.isEmpty()) {
            return "";
        }
        
        StringBuilder sb = new StringBuilder();
        for (String member : membership) {
            sb.append(member).append(',');
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }
    
    private void sendCheckpointIntervalRequest(int checkpointInterval) {
        String request = "CheckpointInterval," + checkpointInterval;
        sendRequestToGroup(replicas, request);
    }
    
    public static void main(String[] args) {
        ReplicaManager node = new ReplicaManager(args[0], args.length >= 2 ? args[1] : null);
        
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println();
            System.out.println("1: set heartbeat interval");
            System.out.println("2: set heartbeat tolerance");
            System.out.println("3: set checkpoint interval");
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
                node.sendCheckpointIntervalRequest(interval);
            }
        }
    }
}

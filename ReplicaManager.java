import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A replica manager which controls the distributed system.
 * @author Shenqi Zhang
 *
 */
public class ReplicaManager extends FaultDetector {
    /**
     * Replicas.
     */
    private Set<String> replicas;
    /**
     * Alive replicas.
     */
    private LinkedHashSet<String> membership;
    /**
     * Lock for membership.
     */
    private final ReentrantReadWriteLock membershipLock;
    
    /**
     * Constructs a replica manager.
     * @param name the name of this node in the distributed system
     */
    public ReplicaManager(String name) {
        this(name, DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_HEARTBEAT_TOLERANCE, null);
    }
    
    /**
     * Constructs a replica manager.
     * @param name the name of this node in the distributed system
     * @param logName the name of the log file; if null, log will be written in stdout
     */
    public ReplicaManager(String name, String logName) {
        this(name, DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_HEARTBEAT_TOLERANCE, logName);
    }
    
    /**
     * Constructs a replica manager.
     * @param name the name of this node in the distributed system
     * @param heartbeatInterval heartbeat interval
     * @param heartbeatTolerance heartbeat tolerance
     */
    public ReplicaManager(String name, int heartbeatInterval, int heartbeatTolerance) {
        this(name, heartbeatInterval, heartbeatTolerance, null);
    }
    
    /**
     * Constructs a replica manager.
     * @param name the name of this node in the distributed system
     * @param heartbeatInterval heartbeat interval
     * @param heartbeatTolerance heartbeat tolerance
     * @param logName the name of the log file; if null, log will be written in stdout
     */
    public ReplicaManager(String name, int heartbeatInterval, int heartbeatTolerance, String logName) {
        super(name, heartbeatInterval, heartbeatTolerance, logName);
        Map<String, String> parameters = getParameters("replica_manager.conf");
        this.replicas = new HashSet<String>();
        this.replicas.addAll(Arrays.asList(parameters.get("replicas").split("\\s+,\\s+")));
        this.membership = new LinkedHashSet<String>();
        this.membershipLock = new ReentrantReadWriteLock();
        
        printParameters();
    }
    
    /**
     * Handles the request from the source.
     * @param source source of the request in the distributed system
     * @param request request
     */
    @Override
    protected void handleRequest(String source, String request) {
        String[] strs = request.split(",");
        String operation = strs[0];
        
        // Alive,<node>
        if (operation.equals("Alive")) {
            String node = strs[1];
            printLog(node + " is alive.");
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
            printLog(node + " is dead.");
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
    
    /**
     * Gets membership request.
     * @return membership request
     */
    private String getMembershipRequest() {
        StringBuilder sb = new StringBuilder("Membership");
        for (String member : membership) {
            sb.append(',').append(member);
        }
        return sb.toString();
    }
    
    /**
     * Gets membership response
     * @return membership response
     */
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
    
    /**
     * Sets heartbeat interval of this distributed system.
     * @param heartbeatInterval heartbeat interval
     */
    @Override
    protected void setHeartbeatInterval(int heartbeatInterval) {
        super.setHeartbeatInterval(heartbeatInterval);
        sendRequestToChildren("HeartbeatInterval," + heartbeatInterval);
    }
    
    /**
     * Sets heartbeat tolerance of this distributed system.
     * @param heartbeatTolerance heartbeat tolerance
     */
    @Override
    protected void setHeartbeatTolerance(int heartbeatTolerance) {
        super.setHeartbeatTolerance(heartbeatTolerance);
        sendRequestToChildren("HeartbeatTolerance," + heartbeatTolerance);
    }
    
    /**
     * Sets checkpoint interval of this distributed system.
     * @param checkpointInterval checkpoint interval
     */
    private void setCheckpointInterval(int checkpointInterval) {
        sendRequestToGroup(replicas, "CheckpointInterval," + checkpointInterval);
    }
    
    /**
     * Prints the parameters.
     */
    @Override
    protected void printParameters() {
        StringBuilder sb = new StringBuilder();
        sb.append("replicas = ");
        if (!this.replicas.isEmpty()) {
            for (String replica : this.replicas) {
                sb.append(replica).append(", ");
            }
            sb.setLength(sb.length() - 2);
        }
        printLog(sb.toString());
    }
    
    /**
     * A shell to test the replica manager.
     * @param args arguments
     */
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
            
            if (operation.equals("1")) {
                System.out.println("Please input heartbeat interval:");
                node.setHeartbeatInterval(Integer.valueOf(scanner.next()));
            } else if (operation.equals("2")) {
                System.out.println("Please input heartbeat tolerance:");
                node.setHeartbeatTolerance(Integer.valueOf(scanner.next()));
            } else if (operation.equals("3")) {
                System.out.println("Please input checkpoint interval:");
                node.setCheckpointInterval(Integer.valueOf(scanner.next()));
            } else {
                System.out.println("Error: Invalid operation!");
                continue;
            }
        }
    }
}

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
        this.replicas.addAll(Arrays.asList(parameters.get("replicas").split("\\s*,\\s*")));
        this.membership = new LinkedHashSet<String>();
        this.membershipLock = new ReentrantReadWriteLock();
        
        printParameters();
    }
    
    /**
     * Serializes membership.
     * @return string representation of membership
     */
    protected String serializeMembership() {
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
        // If heartbeat interval decreases, set children's interval first.
        // If heartbeat interval increases, set the interval of this node first.
        if (heartbeatInterval < this.heartbeatInterval) {
            sendRequestToChildren("HeartbeatInterval|" + heartbeatInterval);
            super.setHeartbeatInterval(heartbeatInterval);
        } else {
            super.setHeartbeatInterval(heartbeatInterval);
            sendRequestToChildren("HeartbeatInterval|" + heartbeatInterval);
        }
    }
    
    /**
     * Sets heartbeat tolerance of this distributed system.
     * @param heartbeatTolerance heartbeat tolerance
     */
    @Override
    protected void setHeartbeatTolerance(int heartbeatTolerance) {
        super.setHeartbeatTolerance(heartbeatTolerance);
        sendRequestToChildren("HeartbeatTolerance|" + heartbeatTolerance);
    }
    
    /**
     * Sets checkpoint interval of this distributed system.
     * @param checkpointInterval checkpoint interval
     */
    private void setCheckpointInterval(int checkpointInterval) {
        sendRequestToGroup(replicas, "CheckpointInterval|" + checkpointInterval);
    }
    
    /**
     * Handles the request from the source.
     * @param source source of the request in the distributed system
     * @param request request
     */
    @Override
    protected void handleRequest(String source, String request) {
        switch (getRequestType(request)) {
        case ALIVE:
            handleAliveRequest(source, request);
            break;
        case DEAD:
            handleDeadRequest(source, request);
            break;
        case MEMBERSHIP:
            handleMembershipRequest(source, request);
            break;
        default:
            printLog(new StringBuilder("Error: Invalid request ").append(request).append('!').toString());
            System.exit(0);
        }
    }
    
    /**
     * Handles the alive request from the source.
     * Alive|<node>
     * @param source source of the request in the distributed system
     * @param request request
     */
    @Override
    protected void handleAliveRequest(String source, String request) {
        String node = request.substring(request.indexOf('|') + 1);
        printLog(node + " is alive.");
        if (replicas.contains(node)) {
            membershipLock.writeLock().lock();
            try {
                if (membership.add(node)) {
                    sendRequestToGroup(membership, "Membership|" + serializeMembership());
                }
            } finally {
                membershipLock.writeLock().unlock();
            }
        }
        sendResponse(source, "ACK");
    }
    
    /**
     * Handles the dead request from the source.
     * Dead|<node>
     * @param source source of the request in the distributed system
     * @param request request
     */
    @Override
    protected void handleDeadRequest(String source, String request) {
        String node = request.substring(request.indexOf('|') + 1);
        printLog(node + " is dead.");
        if (replicas.contains(node)) {
            membershipLock.writeLock().lock();
            try {
                if (membership.remove(node)) {
                    sendRequestToGroup(membership, "Membership|" + serializeMembership());
                }
            } finally {
                membershipLock.writeLock().unlock();
            }
        }
        sendResponse(source, "ACK");
    }
    
    /**
     * Handles the membership request from the source.
     * Membership
     * @param source source of the request in the distributed system
     * @param request request
     */
    protected void handleMembershipRequest(String source, String request) {
        String response;
        membershipLock.readLock().lock();
        try {
            response = serializeMembership();
        } finally {
            membershipLock.readLock().unlock();
        }
        sendResponse(source, response);
    }
    
    /**
     * Prints the parameters.
     */
    private void printParameters() {
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A basic replica which does not handle requests from users.
 * @author Shenqi Zhang
 *
 */
public class Replica extends FaultDetector {
    /**
     * Default checkpoint interval.
     */
    protected static final int DEFAULT_CHECKPOINT_INTERVAL = 5000;
    /**
     * Whether this is the primary replica.
     */
    protected boolean primary;
    /**
     * Data.
     */
    protected Map<String, Integer> data;
    /**
     * Replica manager.
     */
    protected String replicaManager;
    /**
     * Alive replicas.
     */
    protected List<String> membership;
    /**
     * Checkpoint interval.
     */
    protected int checkpointInterval;
    /**
     * Lock for data.
     */
    protected final ReentrantReadWriteLock dataLock;
    /**
     * Lock for membership.
     */
    protected final ReentrantReadWriteLock membershipLock;
    /**
     * Lock for user requests.
     */
    protected final ReentrantReadWriteLock userRequestsLock;
    
    /**
     * Constructs a replica.
     * @param name the name of this node in the distributed system
     */
    public Replica(String name) {
        this(name, DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_HEARTBEAT_TOLERANCE, DEFAULT_CHECKPOINT_INTERVAL, null);
    }
    
    /**
     * Constructs a replica.
     * @param name the name of this node in the distributed system
     * @param logName the name of the log file; if null, log will be written in stdout
     */
    public Replica(String name, String logName) {
        this(name, DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_HEARTBEAT_TOLERANCE, DEFAULT_CHECKPOINT_INTERVAL, logName);
    }
    
    /**
     * Constructs a replica.
     * @param name the name of this node in the distributed system
     * @param heartbeatInterval heartbeat interval
     * @param heartbeatTolerance heartbeat tolerance
     * @param checkpointInterval checkpoint interval
     */
    public Replica(String name, int heartbeatInterval, int heartbeatTolerance, int checkpointInterval) {
        this(name, heartbeatInterval, heartbeatTolerance, checkpointInterval, null);
    }
    
    /**
     * Constructs a replica.
     * @param name the name of this node in the distributed system
     * @param heartbeatInterval heartbeat interval
     * @param heartbeatTolerance heartbeat tolerance
     * @param checkpointInterval checkpoint interval
     * @param logName the name of the log file; if null, log will be written in stdout
     */
    public Replica(String name, int heartbeatInterval, int heartbeatTolerance, int checkpointInterval, String logName) {
        super(name, heartbeatInterval, heartbeatTolerance, logName);
        Map<String, String> parameters = getParameters("replica.conf");
        this.primary = false;
        this.data = new HashMap<String, Integer>();
        for (String str : parameters.get("data").split(",")) {
            int index = str.indexOf(':');
            String key = str.substring(0, index).trim();
            int value = Integer.valueOf(str.substring(index + 1).trim());
            this.data.put(key, value);
        }
        this.replicaManager = parameters.get("replica_manager").trim();
        this.membership = new ArrayList<String>();
        if (checkpointInterval > 0) {
            this.checkpointInterval = checkpointInterval;
        } else {
            this.checkpointInterval = DEFAULT_CHECKPOINT_INTERVAL;
        }
        this.dataLock = new ReentrantReadWriteLock();
        this.membershipLock = new ReentrantReadWriteLock();
        this.userRequestsLock = new ReentrantReadWriteLock();
        
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
        
        // HeartbeatInterval,<interval>
        // HeartbeatTolerance,<tolerance>
        if (operation.equals("HeartbeatInterval") || operation.equals("HeartbeatTolerance")) {
            super.handleRequest(source, request);
        }
        
        // CheckpointInterval,<interval>
        else if (operation.equals("CheckpointInterval")) {
            setCheckpointInterval(Integer.valueOf(strs[1]));
            sendResponse(source, "ACK");
        }
        
        // Membership,<member1>,<member2> ...
        else if (operation.equals("Membership")) {
            membershipLock.writeLock().lock();
            try {
                membership.clear();
                for (int i = 1; i < strs.length; i++) {
                    membership.add(strs[i]);
                }
            } finally {
                membershipLock.writeLock().unlock();
            }
            printMembership();
            sendResponse(source, "ACK");
        }
    }
    
    /**
     * Sets checkpoint interval.
     * @param checkpointInterval checkpoint interval
     */
    private void setCheckpointInterval(int checkpointInterval) {
        if (checkpointInterval > 0) {
            this.checkpointInterval = checkpointInterval;
        }
        printLog("checkpoint interval = " + this.checkpointInterval);
    }
    
    /**
     * Gets the checkpoint
     * @return checkpoint
     */
    protected String getCheckpoint() {
        if (data.isEmpty()) {
            return "";
        }
        
        StringBuilder sb = new StringBuilder();
        for (String key : data.keySet()) {
            sb.append(key).append(',').append(data.get(key)).append(',');
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }
    
    /**
     * Prints the parameters.
     */
    private void printParameters() {
        StringBuilder sb = new StringBuilder();
        sb.append("replica manager = ").append(this.replicaManager).append('\n');
        sb.append("checkpoint interval = ").append(this.checkpointInterval);
        printLog(sb.toString());
    }
    
    /**
     * Prints the data.
     */
    protected void printData() {
        StringBuilder sb = new StringBuilder();
        sb.append("data = ");
        if (!this.data.isEmpty()) {
            for (String key : this.data.keySet()) {
                int value = data.get(key);
                sb.append(key).append(':').append(value).append(", ");
            }
            sb.setLength(sb.length() - 2);
        }
        printLog(sb.toString());
    }
    
    /**
     * Prints the membership.
     */
    protected void printMembership() {
        StringBuilder sb = new StringBuilder("Membership = ");
        if (!this.membership.isEmpty()) {
            for (String member : this.membership) {
                sb.append(member).append(", ");
            }
            sb.setLength(sb.length() - 2);
        }
        printLog(sb.toString());
    }
    
    /**
     * Launches a replica
     * @param args arguments
     */
    public static void main(String[] args) {
        new Replica(args[0], args.length >= 2 ? args[1] : null);
    }
}

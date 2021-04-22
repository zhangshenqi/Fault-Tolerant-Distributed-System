import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A basic replica.
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
        
        printParameters();
    }
    
    /**
     * Serializes data.
     * @return string representation of data
     */
    protected String serializeData() {
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
     * Deserializes data.
     * @param s string representation of data
     */
    protected void deserializeData(String s) {
        data.clear();
        if (s.length() > 0) {
            String[] strs = s.split(",");
            for (int i = 0; i < strs.length; i += 2) {
                String key = strs[i];
                int value = Integer.valueOf(strs[i + 1]);
                data.put(key, value);
            }
        }
    }
    
    /**
     * Deserializes membership.
     * @param s string representation of membership
     */
    protected void deserializeMembership(String s) {
        membership.clear();
        if (s.length() > 0) {
            String[] strs = s.split(",");
            for (String member : strs) {
                membership.add(member);
            }
        }
    }
    
    /**
     * Returns the value to which the specified key is mapped, or null if the data contains no mapping for the key.
     * @param key key
     * @return the value to which the specified key is mapped, or null if the data contains no mapping for the key
     */
    protected Integer get(String key) {
        return data.get(key);
    }
    
    /**
     * Increments the value to which the specified key is mapped.
     * @param key key
     * @return the value to which the specified key is mapped after the increment, or null if the data contains no mapping for the key
     */
    protected Integer increment(String key) {
        Integer value = data.get(key);
        if (value == null) {
            return null;
        }
        
        value++;
        data.put(key, value);
        return value;
    }
    
    /**
     * Decrements the value to which the specified key is mapped.
     * @param key key
     * @return the value to which the specified key is mapped after the decrement, or null if the data contains no mapping for the key
     */
    protected Integer decrement(String key) {
        Integer value = data.get(key);
        if (value == null) {
            return null;
        }
        
        value--;
        data.put(key, value);
        return value;
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
     * Handles the request from the source.
     * @param source source of the request in the distributed system
     * @param request request
     */
    @Override
    protected void handleRequest(String source, String request) {
        switch (getRequestType(request)) {
        case MEMBERSHIP:
            handleMembershipRequest(source, request);
            break;
        case HEARTBEAT_INTERVAL:
            handleHeartbeatIntervalRequest(source, request);
            break;
        case HEARTBEAT_TOLERANCE:
            handleHeartbeatToleranceRequest(source, request);
            break;
        case CHECKPOINT_INTERVAL:
            handleCheckpointIntervalRequest(source, request);
            break;
        case GET:
            handleGetRequest(source, request);
            break;
        case INCREMENT:
            handleIncrementRequest(source, request);
            break;
        case DECREMENT:
            handleDecrementRequest(source, request);
            break;
        default:
            printLog(new StringBuilder("Error: Invalid request ").append(request).append('!').toString());
            System.exit(0);
        }
    }
    
    /**
     * Handles the membership interval request from the source.
     * Membership|<member1>,<member2> ...
     * @param source source of the request in the distributed system
     * @param request request
     */
    protected void handleMembershipRequest(String source, String request) {
        int index = request.indexOf('|');
        String membershipStr = index == request.length() - 1 ? "" : request.substring(index + 1);
        membershipLock.writeLock().lock();
        try {
            deserializeMembership(membershipStr);
        } finally {
            membershipLock.writeLock().unlock();
        }
        printMembership();
        sendResponse(source, "ACK");
    }
    
    /**
     * Handles the checkpoint interval request from the source.
     * Checkpoint|<node>
     * @param source source of the request in the distributed system
     * @param request request
     */
    protected void handleCheckpointIntervalRequest(String source, String request) {
        int interval = Integer.valueOf(request.substring(request.indexOf('|') + 1));
        setCheckpointInterval(Integer.valueOf(interval));
        sendResponse(source, "ACK");
    }
    
    /**
     * Handles the get request from the source.
     * Get|<key>
     * @param source source of the request in the distributed system
     * @param request request
     */
    protected void handleGetRequest(String source, String request) {
        String key = request.substring(request.indexOf('|') + 1, request.indexOf(','));
        Integer value;
        dataLock.readLock().lock();
        try {
            value = get(key);
        } finally {
            dataLock.readLock().unlock();
        }
        String response = value == null ? "No such key." : String.valueOf(value);
        sendResponse(source, response);
    }
    
    /**
     * Handles the increment request from the source.
     * Increment|<key>
     * @param source source of the request in the distributed system
     * @param request request
     */
    protected void handleIncrementRequest(String source, String request) {
        String key = request.substring(request.indexOf('|') + 1, request.indexOf(','));
        Integer value;
        dataLock.writeLock().lock();
        try {
            value = increment(key);
        } finally {
            dataLock.writeLock().unlock();
        }
        String response = value == null ? "No such key." : String.valueOf(value);
        sendResponse(source, response);
    }
    
    /**
     * Handles the decrement request from the source.
     * Decrement|<key>
     * @param source source of the request in the distributed system
     * @param request request
     */
    protected void handleDecrementRequest(String source, String request) {
        String key = request.substring(request.indexOf('|') + 1, request.indexOf(','));
        Integer value;
        dataLock.writeLock().lock();
        try {
            value = decrement(key);
        } finally {
            dataLock.writeLock().unlock();
        }
        String response = value == null ? "No such key." : String.valueOf(value);
        sendResponse(source, response);
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

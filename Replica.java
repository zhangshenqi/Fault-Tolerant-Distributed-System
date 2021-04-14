import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Replica extends FaultDetector {
    protected boolean primary;
    protected Map<String, Integer> data;
    protected String replicaManager;
    protected List<String> membership;
    protected int checkpointInterval;
    protected final ReentrantReadWriteLock dataLock;
    protected final ReentrantReadWriteLock membershipLock;
    protected final ReentrantReadWriteLock userRequestsLock;
    
    public Replica(String name) {
        this(name, 1000, 3, 5000, null);
    }
    
    public Replica(String name, String connectionManagerLogName) {
        this(name, 1000, 3, 5000, connectionManagerLogName);
    }
    
    public Replica(String name, int heartbeatInterval, int heartbeatTolerance, int checkpointInterval) {
        this(name, heartbeatInterval, heartbeatTolerance, checkpointInterval, null);
    }
    
    /**
     * Constructs a fault detector with specified name and connection manager log name.
     * @param name name of this sample node.
     * @param logName name of the connection manager log file
     */
    public Replica(String name, int heartbeatInterval, int heartbeatTolerance, int checkpointInterval, String connectionManagerLogName) {
        super(name, heartbeatInterval, heartbeatTolerance, connectionManagerLogName);
        Map<String, String> parameters = getParameters("replica.conf");
        this.primary = false;
        this.data = new HashMap<String, Integer>();
        for (String str : parameters.get("data").split(",")) {
            int index = str.indexOf(':');
            String key = str.substring(0, index).trim();
            int value = Integer.valueOf(str.substring(index + 1).trim());
            data.put(key, value);
        }
        this.replicaManager = parameters.get("replica_manager").trim();
        this.membership = new ArrayList<String>();
        this.checkpointInterval = checkpointInterval;
        this.dataLock = new ReentrantReadWriteLock();
        this.membershipLock = new ReentrantReadWriteLock();
        this.userRequestsLock = new ReentrantReadWriteLock();
    }
    
    @Override
    protected void handleRequest(String source, String request) {
        String[] strs = request.split(",");
        String operation = strs[0];
        
        if (operation.equals("HeartbeatInterval") || operation.equals("HeartbeatTolerance")) {
            super.handleRequest(source, request);
        }
        
        else if (operation.equals("Membership")) {
            sendResponse(source, "ACK");
            membershipLock.writeLock().lock();
            try {
                membership.clear();
                for (int i = 1; i < strs.length; i++) {
                    membership.add(strs[i]);
                }
            } finally {
                membershipLock.writeLock().unlock();
            }
        }
        
        else if (operation.equals("CheckpointInterval")) {
            sendResponse(source, "ACK");
            checkpointInterval = Integer.valueOf(strs[1]);
        }
    }
    
    protected String getCheckpoint() {
        StringBuilder sb = new StringBuilder();
        for (String key : data.keySet()) {
            sb.append(key).append(',').append(data.get(key)).append(',');
        }
        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }
    
    public static void main(String[] args) {
        new Replica(args[0], args.length >= 2 ? args[1] : null);
    }
}

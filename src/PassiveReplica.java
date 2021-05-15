import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A replica which performs passive replication.
 * @author Shenqi Zhang
 *
 */
public class PassiveReplica extends Replica {
    /**
     * User requests.
     * Keys are users. Values are queues which store requests from each user.
     * In each queue, requests are in chronological order.
     */
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<String>> userRequests;
    /**
     * Timestamps of the latest handled request from each user after the latest checkpoint.
     * Keys are users. Values are timestamps.
     * It is assumed that, for the same user, requests have different timestamps.
     */
    private Map<String, Long> userTimestamps;
    /**
     * True if this replica is not updating.
     */
    private boolean updated;
    /**
     * Object for update.
     */
    private final Object updateObj;
    /**
     * Object for upgrade.
     */
    private final Object upgradeObj;
    
    /**
     * Constructs a passive replica.
     * @param name the name of this node in the distributed system
     */
    public PassiveReplica(String name) {
        this(name, DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_HEARTBEAT_TOLERANCE, DEFAULT_CHECKPOINT_INTERVAL, null);
    }
    
    /**
     * Constructs a passive replica.
     * @param name the name of this node in the distributed system
     * @param logName the name of the log file; if null, log will be written in stdout
     */
    public PassiveReplica(String name, String logName) {
        this(name, DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_HEARTBEAT_TOLERANCE, DEFAULT_CHECKPOINT_INTERVAL, logName);
    }
    
    /**
     * Constructs a passive replica.
     * @param name the name of this node in the distributed system
     * @param heartbeatInterval
     * @param heartbeatTolerance
     * @param checkpointInterval
     */
    public PassiveReplica(String name, int heartbeatInterval, int heartbeatTolerance, int checkpointInterval) {
        this(name, heartbeatInterval, heartbeatTolerance, checkpointInterval, null);
    }
    
    /**
     * Constructs a passive replica.
     * @param name the name of this node in the distributed system
     * @param heartbeatInterval
     * @param heartbeatTolerance
     * @param checkpointInterval
     * @param logName the name of the log file; if null, log will be written in stdout
     */
    public PassiveReplica(String name, int heartbeatInterval, int heartbeatTolerance, int checkpointInterval, String logName) {
        super(name, heartbeatInterval, heartbeatTolerance, checkpointInterval, logName);
        this.userRequests = new ConcurrentHashMap<String, ConcurrentLinkedQueue<String>>();
        this.userTimestamps = new HashMap<String, Long>();
        this.updated = true;
        this.updateObj = new Object();
        this.upgradeObj = new Object();
    }
    
    /**
     * Serializes user timestamps.
     * @return string representation of user timestamps
     */
    private String serializeUserTimestamps() {
        if (userTimestamps.isEmpty()) {
            return "";
        }
        
        StringBuilder sb = new StringBuilder();
        for (String user : userTimestamps.keySet()) {
            sb.append(user).append(',').append(userTimestamps.get(user)).append(',');
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }
    
    /**
     * Deserializes user timestamps.
     * @param s string representation of user timestamps
     */
    private void deserializeUserTimestamps(String s) {
        userTimestamps.clear();
        if (s.length() > 0) {
            String[] strs = s.split(",");
            for (int i = 0; i < strs.length; i += 2) {
                String user = strs[i];
                long timestamp = Long.valueOf(strs[i + 1]);
                userTimestamps.put(user, timestamp);
            }
        }
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
        case CHECKPOINT:
            handleCheckpointRequest(source, request);
            break;
        case UPGRADED:
            handleUpgradedRequest(source, request);
            break;
        default:
            printLog(new StringBuilder("Error: Invalid request ").append(request).append('!').toString());
            System.exit(0);
        }
    }
    
    /**
     * Handles the membership request from the source.
     * Membership|<member1>,<member2> ...
     * @param source source of the request in the distributed system
     * @param request request
     */
    @Override
    protected void handleMembershipRequest(String source, String request) {
        int index = request.indexOf('|');
        String membershipStr = index == request.length() - 1 ? "" : request.substring(index + 1);
        membershipLock.writeLock().lock();
        try {
            deserializeMembership(membershipStr);
            printMembership();
            
            if (!primary && membership.get(0).equals(name)) {
                printLog("Upgrade from backup to primary.");
                waitForUpdate();
                reHandleUserRequests();
                primary = true;
                
                synchronized(upgradeObj) {
                    upgradeObj.notifyAll();
                }
                
                new Thread(new CheckpointSender()).start();
            }
        } finally {
            membershipLock.writeLock().unlock();
        }
        sendResponse(source, "ACK");
    }
    
    /**
     * Handles the get request from the source.
     * Get|<key>,<timestamp>
     * @param source source of the request in the distributed system
     * @param request request
     */
    @Override
    protected void handleGetRequest(String source, String request) {
        if (primary) {
            super.handleGetRequest(source, request);
        } else {
            sendResponse(source, "ACK");
        }
    }
    
    /**
     * Handles the increment request from the source.
     * Increment|<key>,<timestamp>
     * @param source source of the request in the distributed system
     * @param request request
     */
    @Override
    protected void handleIncrementRequest(String source, String request) {
        if (primary) {
            int index1 = request.indexOf('|');
            int index2 = request.indexOf(',');
            String key = request.substring(index1 + 1, index2);
            long timestamp = Long.valueOf(request.substring(index2 + 1));
            Integer value;
            dataLock.writeLock().lock();
            try {
                value = increment(key);
                userTimestamps.put(source, timestamp);
            } finally {
                dataLock.writeLock().unlock();
            }
            String response = value == null ? "No such key." : String.valueOf(value);
            sendResponse(source, response);
        } else {
            userRequests.putIfAbsent(source, new ConcurrentLinkedQueue<String>());
            userRequests.get(source).add(request);
            sendResponse(source, "ACK");
        }
    }
    
    /**
     * Handles the decrement request from the source.
     * Decrement|<key>,<timestamp>
     * @param source source of the request in the distributed system
     * @param request request
     */
    protected void handleDecrementRequest(String source, String request) {
        if (primary) {
            int index1 = request.indexOf('|');
            int index2 = request.indexOf(',');
            String key = request.substring(index1 + 1, index2);
            long timestamp = Long.valueOf(request.substring(index2 + 1));
            Integer value;
            dataLock.writeLock().lock();
            try {
                value = decrement(key);
                userTimestamps.put(source, timestamp);
            } finally {
                dataLock.writeLock().unlock();
            }
            String response = value == null ? "No such key." : String.valueOf(value);
            sendResponse(source, response);
        } else {
            userRequests.putIfAbsent(source, new ConcurrentLinkedQueue<String>());
            userRequests.get(source).add(request);
            sendResponse(source, "ACK");
        }
    }
    
    /**
     * Handles the checkpoint request from the source.
     * Checkpoint|<key1>,<value1>,<key2>,<value2> ... |<user1>,<timestamp1>,<user2>,<timestamp2>, ...
     * @param source source of the request in the distributed system
     * @param request request
     */
    protected void handleCheckpointRequest(String source, String request) {
        // Only backup replicas receive checkpoint requests.
        // This is the only thread to access data and user timestamps.
        // No need to add locks.
        int index1 = request.indexOf('|');
        int index2 = request.indexOf('|', index1 + 1);
        String dataStr = index2 == index1 + 1 ? "" : request.substring(index1 + 1, index2);
        String userTimeStampsStr = index2 == request.length() - 1 ? "" : request.substring(index2 + 1);
        updated = false;
        deserializeData(dataStr);
        printData();
        deserializeUserTimestamps(userTimeStampsStr);
        printUserTimestamps();
        updateUserRequests();
        updated = true;
        
        synchronized(updateObj) {
            updateObj.notify();
        }
        
        sendResponse(source, "ACK");
    }
    
    /**
     * Handles the upgraded request from the source.
     * Upgraded
     * @param source source of the request in the distributed system
     * @param request request
     */
    protected void handleUpgradedRequest(String source, String request) {
        waitForUpgrade();
        sendResponse(source, "ACK");
    }
    
    /**
     * Re-handles user requests concurrently.
     */
    private void reHandleUserRequests() {
        List<Thread> threads = new ArrayList<Thread>(userRequests.size());
        for (String user : userRequests.keySet()) {
            threads.add(new Thread(new UserRequestsReHandler(user, userRequests.get(user))));
        }
        startAndJoinThreads(threads);
    }
    
    /**
     * Updates requests of each user concurrently.
     */
    private void updateUserRequests() {
        List<Thread> threads = new ArrayList<Thread>(userTimestamps.size());
        for (String user : userTimestamps.keySet()) {
            threads.add(new Thread(new UserRequestsUpdater(user, userRequests.get(user), userTimestamps.get(user))));
        }
        startAndJoinThreads(threads);
    }
    
    /**
     * Waits for update.
     */
    private void waitForUpdate() {
        synchronized(updateObj) {
            while (!updated) {
                printLog("Wait for update.");
                try {
                    updateObj.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    
    /**
     * Waits for upgrade.
     */
    private void waitForUpgrade() {
        synchronized(upgradeObj) {
            while (!primary) {
                printLog("Wait for upgrade.");
                try {
                    upgradeObj.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    
    /**
     * Prints the user timestamps.
     */
    protected void printUserTimestamps() {
        StringBuilder sb = new StringBuilder();
        sb.append("user_timestamps = ");
        if (!this.userTimestamps.isEmpty()) {
            for (String user : this.userTimestamps.keySet()) {
                sb.append(user).append(':').append(this.userTimestamps.get(user)).append(", ");
            }
            sb.setLength(sb.length() - 2);
        }
        printLog(sb.toString());
    }
    
    /**
     * Checkpoint sender.
     *
     */
    private class CheckpointSender implements Runnable {
        /**
         * Primary replica sends checkpoint to backups periodically.
         */
        @Override
        public void run() {
            printLog("Launch checkpoint sender.");
            String request;
            while (true) {
                dataLock.readLock().lock();
                try {
                    request = new StringBuilder("Checkpoint|").append(serializeData()).append('|').append(serializeUserTimestamps()).toString();
                    userTimestamps.clear();
                } finally {
                    dataLock.readLock().unlock();
                }
                
                membershipLock.readLock().lock();
                try {
                    sendRequestToGroup(membership, name, request);
                } finally {
                    membershipLock.readLock().unlock();
                }
                
                try {
                    Thread.sleep(checkpointInterval);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    
    /**
     * User requests re-handler.
     *
     */
    private class UserRequestsReHandler implements Runnable {
        /**
         * User.
         */
        private String user;
        /**
         * Requests.
         */
        private ConcurrentLinkedQueue<String> requests;
        
        /**
         * Constructs a user request re-handler.
         * @param user user
         * @param requests requests
         */
        UserRequestsReHandler(String user, ConcurrentLinkedQueue<String> requests) {
            this.user = user;
            this.requests = requests;
        }
        
        /**
         * Re-handles user requests.
         */
        @Override
        public void run() {
            printLog(new StringBuilder("Launch user requests re-handler for ").append(user).append('.').toString());
            while (!requests.isEmpty()) {
                String request = requests.poll();
                String key = request.substring(request.indexOf('|') + 1, request.indexOf(','));
                
                switch (getRequestType(request)) {
                case INCREMENT:
                    dataLock.writeLock().lock();
                    try {
                        increment(key);
                    } finally {
                        dataLock.writeLock().unlock();
                    }
                    break;
                case DECREMENT:
                    dataLock.writeLock().lock();
                    try {
                        decrement(key);
                    } finally {
                        dataLock.writeLock().unlock();
                    }
                    break;
                default:
                    printLog(new StringBuilder("Error: Invalid user request ").append(request).append('!').toString());
                    System.exit(0);
                }
                
                printLog(new StringBuilder("Re-handle ").append(request).append(" from ").append(user).append('.').toString());
            }
        }
    }
    
    /**
     * User requests updater.
     *
     */
    private class UserRequestsUpdater implements Runnable {
        /**
         * User.
         */
        private String user;
        /**
         * Requests.
         */
        private ConcurrentLinkedQueue<String> requests;
        /**
         * Target timestamp.
         */
        private long targetTimestamp;
        
        /**
         * Constructs a user request updater.
         * @param user user
         * @param requests requests
         * @param targetTimestamp targetTimestamp
         */
        UserRequestsUpdater(String user, ConcurrentLinkedQueue<String> requests, long targetTimestamp) {
            this.user = user;
            this.requests = requests;
            this.targetTimestamp = targetTimestamp;
        }
        
        /**
         * Removes requests no after the target timestamp.
         */
        @Override
        public void run() {
            printLog(new StringBuilder("Launch user requests updater for ").append(user).append('.').toString());
            while (!requests.isEmpty()) {
                String request = requests.peek();
                long timestamp = Long.valueOf(request.substring(request.indexOf(',') + 1));
                if (timestamp > targetTimestamp) {
                    return;
                }
                requests.poll();
                printLog(new StringBuilder("Remove ").append(request).append(" from ").append(user).append('.').toString());
            }
        }
    }
    
    /**
     * Launches a passive replica.
     * @param args arguments
     */
    public static void main(String[] args) {
        new PassiveReplica(args[0], args.length >= 2 ? args[1] : null);
    }
}

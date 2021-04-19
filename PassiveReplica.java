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
     */
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<String>> userRequests;
    /**
     * Timestamps of the latest handled request from each user after the latest checkpoint.
     * Keys are users. Values are timestamps.
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
     * @param logName
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
     * @param logName
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
        // CheckpointInterval,<interval>
        if (operation.equals("HeartbeatInterval") || operation.equals("HeartbeatTolerance") || operation.equals("CheckpointInterval")) {
            super.handleRequest(source, request);
        }
        
        // Membership,<member1>,<member2> ...
        else if (operation.equals("Membership")) {
            membershipLock.writeLock().lock();
            try {
                membership.clear();
                for (int i = 1; i < strs.length; i++) {
                    membership.add(strs[i]);
                }
                printMembership();
                
                if (!primary && membership.get(0).equals(name)) {
                    printLog("Upgrade from back to primary.");
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
        
        // Checkpoint,<key1>,<value1>,<key2>,<value2> ... |<user1>,<timestamp1>,<user2>,<timestamp2>, ...
        else if (operation.equals("Checkpoint")) {
            // Only backup replicas receive Checkpoint request.
            // This is the only thread to access data and user timestamps.
            // No need to add locks.
            int index1 = request.indexOf(',');
            int index2 = request.indexOf('|');
            String dataRequest = index2 == index1 + 1 ? "" : request.substring(index1 + 1, index2);
            String userTimeStampRequest = index2 == request.length() - 1 ? "" : request.substring(index2 + 1);
            
            updated = false;
            updateData(dataRequest);
            updateUserTimestamps(userTimeStampRequest);
            updateUserRequests();
            updated = true;
            
            synchronized(updateObj) {
                updateObj.notify();
            }
            
            sendResponse(source, "ACK");
        }
        
        // Upgraded
        else if (operation.equals("Upgraded")) {
            waitForUpgrade();
            sendResponse(source, "ACK");
        }
        
        // Get,<key>,<timestamp>
        else if (operation.equals("Get")) {
            if (primary) {
                String key = strs[1];
                int value;
                dataLock.readLock().lock();
                try {
                    value = data.get(key);
                } finally {
                    dataLock.readLock().unlock();
                }
                sendResponse(source, String.valueOf(value));
            } else {
                sendResponse(source, "ACK");
            }
        }
        
        // Increment,<key>,<timestamp>
        else if (operation.equals("Increment")) {
            if (primary) {
                String key = strs[1];
                String response;
                dataLock.writeLock().lock();
                try {
                    if (data.containsKey(key)) {
                        data.put(key, data.get(key) + 1);
                        response = String.valueOf(data.get(key));
                    } else {
                        response = "No such key.";
                    }
                    userTimestamps.put(source, Long.valueOf(strs[2]));
                } finally {
                    dataLock.writeLock().unlock();
                }
                sendResponse(source, response);
            } else {
                userRequests.putIfAbsent(source, new ConcurrentLinkedQueue<String>());
                userRequests.get(source).add(request);
                sendResponse(source, "ACK");
            }
        }
        
        // Decrement,<key>,<timestamp>
        else if (operation.equals("Decrement")) {
            if (primary) {
                String key = strs[1];
                String response;
                dataLock.writeLock().lock();
                try {
                    if (data.containsKey(key)) {
                        data.put(key, data.get(key) - 1);
                        response = String.valueOf(data.get(key));
                    } else {
                        response = "No such key.";
                    }
                    userTimestamps.put(source, Long.valueOf(strs[2]));
                } finally {
                    dataLock.writeLock().unlock();
                }
                sendResponse(source, response);
            } else {
                userRequests.putIfAbsent(source, new ConcurrentLinkedQueue<String>());
                userRequests.get(source).add(request);
                sendResponse(source, "ACK");
            }
        }
    }
    
    /**
     * Re-handle user requests concurrently.
     */
    private void reHandleUserRequests() {
        List<Thread> threads = new ArrayList<Thread>(userRequests.size());
        for (String user : userRequests.keySet()) {
            threads.add(new Thread(new UserRequestsReHandler(user, userRequests.get(user))));
        }
        startAndJoinThreads(threads);
    }
    
    /**
     * Updates data.
     * @param request data request from the primary replica.
     */
    private void updateData(String request) {
        data.clear();
        if (request.length() > 0) {
            String[] strs = request.split(",");
            for (int i = 0; i < strs.length; i += 2) {
                String key = strs[i];
                int value = Integer.valueOf(strs[i + 1]);
                data.put(key, value);
            }
        }
        printData();
    }
    
    /**
     * Updates previous user requests.
     * @param str string representation of previous user requests.
     */
    private void updateUserTimestamps(String request) {
        userTimestamps.clear();
        if (request.length() > 0) {
            String[] strs = request.split(",");
            for (int i = 0; i < strs.length; i += 2) {
                String user = strs[i];
                long timestamp = Integer.valueOf(strs[i + 1]);
                userTimestamps.put(user, timestamp);
            }
        }
        printUserTimestamps();
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
     * Wait for update.
     */
    private void waitForUpdate() {
        synchronized(updateObj) {
            while (!updated) {
                try {
                    updateObj.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    
    /**
     * Wait for upgrade.
     */
    private void waitForUpgrade() {
        synchronized(upgradeObj) {
            while (!primary) {
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
                sb.append(user).append(':').append(userTimestamps.get(user)).append(", ");
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
         * Gets checkpoint request.
         * @return checkpoint request
         */
        private String getCheckpointRequest() {
            StringBuilder sb = new StringBuilder("Checkpoint,");
            sb.append(getCheckpoint()).append('|');
            if (!userTimestamps.isEmpty()) {
                for (String user : userTimestamps.keySet()) {
                    sb.append(user).append(',').append(userTimestamps.get(user)).append(',');
                }
                sb.setLength(sb.length() - 1);
            }
            return sb.toString();
        }
        
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
                    request = getCheckpointRequest();
                    userTimestamps.clear();
                } finally {
                    dataLock.readLock().unlock();
                }
                
                membershipLock.readLock().lock();
                try {
                    int size = membership.size();
                    for (int i = 1; i < size; i++) {
                        sendRequest(membership.get(i), request);
                    }
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
         * Remove requests no after the target timestamp.
         */
        @Override
        public void run() {
            printLog(new StringBuilder("Launch user requests updater for ").append(user).append('.').toString());
            while (!requests.isEmpty()) {
                String request = requests.peek();
                long timestamp = Long.valueOf(request.substring(request.lastIndexOf(',') + 1));
                if (timestamp > targetTimestamp) {
                    return;
                }
                requests.poll();
                printLog(new StringBuilder("Remove ").append(request).append(" from ").append(user).append('.').toString());
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
         * Re-handle user requests.
         */
        @Override
        public void run() {
            printLog(new StringBuilder("Launch user requests re-handler for ").append(user).append('.').toString());
            while (!requests.isEmpty()) {
                String request = requests.poll();
                String[] strs = request.split(",");
                String operation = strs[0];
                String key = strs[1];
                
                // Increment,<key>,<timestamp>
                if (operation.equals("Increment")) {
                    dataLock.writeLock().lock();
                    try {
                        if (data.containsKey(key)) {
                            data.put(key, data.get(key) + 1);
                        }
                    } finally {
                        dataLock.writeLock().unlock();
                    }
                }
                
                // Decrement,<key>,<timestamp>
                else if (operation.equals("Decrement")) {
                    try {
                        if (data.containsKey(key)) {
                            data.put(key, data.get(key) - 1);
                        }
                    } finally {
                        dataLock.writeLock().unlock();
                    }
                }
                
                printLog(new StringBuilder("Re-handle ").append(request).append(" from ").append(user).append('.').toString());
            }
        }
    }
    
    /**
     * Launches a passive replica
     * @param args arguments
     */
    public static void main(String[] args) {
        new PassiveReplica(args[0], args.length >= 2 ? args[1] : null);
    }
}

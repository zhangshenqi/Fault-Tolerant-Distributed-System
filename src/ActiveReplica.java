import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A replica which performs active replication.
 * @author Shenqi Zhang
 *
 */
public class ActiveReplica extends Replica {
    /**
     * User requests.
     */
    private LinkedHashSet<String> userRequests;
    /**
     * User requests restored from another replica.
     */
    private Set<String> restoredUserRequests;
    /**
     * Current user request.
     */
    private String currentUserRequest;
    /**
     * Previous user request.
     */
    private String previousUserRequest;
    /**
     * Checkpoint.
     */
    private String checkpoint;
    /**
     * Log builder.
     */
    private StringBuilder logBuilder;
    /**
     * True if this replica finished restoration.
     */
    private boolean restored;
    /**
     * True if this replica is in quiescent state.
     */
    private boolean quiescent;
    /**
     * Lock for user requests.
     */
    private final ReentrantReadWriteLock userRequestsLock;
    /**
     * Object for restoration.
     */
    private final Object restorationObj;
    /**
     * Object for Quiescence.
     */
    private final Object quiescenceObj;
    /**
     * Object for user requests.
     */
    private final Object userRequestsObj;
    
    /**
     * Constructs an active replica.
     * @param name the name of this node in the distributed system
     */
    public ActiveReplica(String name) {
        this(name, DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_HEARTBEAT_TOLERANCE, DEFAULT_CHECKPOINT_INTERVAL, null);
    }
    
    /**
     * Constructs an active replica.
     * @param name the name of this node in the distributed system
     * @param logName the name of the log file; if null, log will be written in stdout
     */
    public ActiveReplica(String name, String logName) {
        this(name, DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_HEARTBEAT_TOLERANCE, DEFAULT_CHECKPOINT_INTERVAL, logName);
    }
    
    /**
     * Constructs an active replica.
     * @param name the name of this node in the distributed system
     * @param heartbeatInterval heartbeat interval
     * @param heartbeatTolerance heartbeat tolerance
     * @param checkpointInterval checkpoint interval
     */
    public ActiveReplica(String name, int heartbeatInterval, int heartbeatTolerance, int checkpointInterval) {
        this(name, heartbeatInterval, heartbeatTolerance, checkpointInterval, null);
    }
    
    /**
     * Constructs an active replica.
     * @param name the name of this node in the distributed system
     * @param heartbeatInterval heartbeat interval
     * @param heartbeatTolerance heartbeat tolerance
     * @param checkpointInterval checkpoint interval
     * @param logName the name of the log file; if null, log will be written in stdout
     */
    public ActiveReplica(String name, int heartbeatInterval, int heartbeatTolerance, int checkpointInterval, String logName) {
        super(name, heartbeatInterval, heartbeatTolerance, checkpointInterval, logName);
        this.userRequests = new LinkedHashSet<String>();
        this.restoredUserRequests = new HashSet<String>();
        this.currentUserRequest = "";
        this.previousUserRequest = "";
        this.checkpoint = serializeData();
        this.logBuilder = new StringBuilder();
        this.restored = false;
        this.quiescent = false;
        this.userRequestsLock = new ReentrantReadWriteLock();
        this.restorationObj = new Object();
        this.quiescenceObj = new Object();
        this.userRequestsObj = new Object();
    }
    
    /**
     * Serializes user requests.
     * @return string representation of user requests
     */
    private String serializeUserRequests() {
        if (userRequests.isEmpty()) {
            return "";
        }
        
        StringBuilder sb = new StringBuilder();
        for (String userRequest : userRequests) {
            sb.append(userRequest).append(';');
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }
    
    /**
     * Adds a user request.
     * @param source source of the request in the distributed system
     * @param request request
     */
    protected void addUserRequest(String source, String request) {
        userRequestsLock.writeLock().lock();
        try {
            userRequests.add(new StringBuilder(request).append(",").append(source).toString());
        } finally {
            userRequestsLock.writeLock().unlock();
        }
        
        if (primary) {
            synchronized(userRequestsObj) {
                userRequestsObj.notify();
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
        case VOTE:
            handleVoteRequest(source, request);
            break;
        case DO:
            handleDoRequest(source, request);
            break;
        case GIVE_UP:
            handleGiveUpRequest(source, request);
            break;
        case BLOCK:
            handleBlockRequest(source, request);
            break;
        case UNBLOCK:
            handleUnblockRequest(source, request);
            break;
        case RESTORE:
            handleRestoreRequest(source, request);
            break;
        case CURRENT:
            handleCurrentRequest(source, request);
            break;
        case PREVIOUS:
            handlePreviousRequest(source, request);
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
            
            if (!restored) {
                // If this is not the first replica, restore data and user requests from another replica.
                if (membership.size() > 1) {
                    printLog("Restore.");
                    sendRequestToGroup(membership, name, "Block");
                    String response;
                    for (String member : membership) {
                        if (member.equals(name)) {
                            continue;
                        }
                        if ((response = sendRequest(member, "Restore")) != null) {
                            int index1 = response.indexOf('&');
                            int index2 = response.indexOf('&', index1 + 1);
                            String checkpointResponse = index1 == 0 ? "" : response.substring(0, index1);
                            String logResponse = index2 == index1 + 1 ? "" : response.substring(index1 + 1, index2);
                            String userRequestsResponse = index2 == response.length() - 1 ? "" : response.substring(index2 + 1);
                            restoreData(checkpointResponse, logResponse);
                            checkpoint = serializeData();
                            restoreUserRequests(userRequestsResponse);
                            break;
                        }
                    }
                    printData();
                    sendRequestToGroup(membership, name, "Unblock");
                }
                
                restored = true;
                synchronized(restorationObj) {
                    restorationObj.notify();
                }
                new Thread(new CheckpointUpdater()).start();
            }
            
            if (!primary && membership.get(0).equals(name)) {
                printLog("Become the primary.");
                primary = true;
                // There is a corner case.
                // The previous primary replica dies when some do requests are sent and some are not.
                // The new primary replica needs to do a synchronization.
                Map<String, String> responses = sendRequestToGroup(membership, "Current");
                List<String> busyMembers = new ArrayList<String>();
                List<String> freeMembers = new ArrayList<String>();
                Set<String> currentUserRequests = new HashSet<String>();
                for (String member : responses.keySet()) {
                    String currentUserRequest = responses.get(member);
                    if (currentUserRequest != null) {
                        if (currentUserRequest.isEmpty()) {
                            freeMembers.add(member);
                        } else {
                            busyMembers.add(member);
                            currentUserRequests.add(currentUserRequest);
                        }
                    }
                }
                
                if (!currentUserRequests.isEmpty()) {
                    // This should never happen.
                    if (currentUserRequests.size() > 1) {
                        printLog("Error: Replicas have different current user requests!");
                        System.exit(0);
                    }
                    
                    if (freeMembers.isEmpty()) {
                        sendRequestToGroup(busyMembers, "Do|" + currentUserRequests.iterator().next());
                    }
                    
                    else {
                        responses = sendRequestToGroup(freeMembers, "Previous");
                        Set<String> previousUserRequests = new HashSet<String>();
                        for (String previousUserRequest : responses.values()) {
                            if (previousUserRequest != null && !previousUserRequest.isEmpty()) {
                                previousUserRequests.add(previousUserRequest);
                            }
                        }
                        
                        if (!previousUserRequests.isEmpty()) {
                            // This should never happen.
                            if (previousUserRequests.size() > 1) {
                                printLog("Error: Replicas have different previous user requests!");
                                System.exit(0);
                            }
                            
                            // If the previous user requests of free replicas are the same with the current user requests of busy replicas,
                            // the previous primary replica dies during sending do request.
                            // Otherwise, the previous primary replica dies during sending vote request.
                            if (previousUserRequests.iterator().next().equals(currentUserRequests.iterator().next())) {
                                sendRequestToGroup(busyMembers, "Do|" + currentUserRequests.iterator().next());
                            }
                        }
                    }
                }
                new Thread(new VoteInitiator()).start();
            }
        } finally {
            membershipLock.writeLock().unlock();
        }
        sendResponse(source, "ACK");
    }
    
    /**
     * Handles the get request from the source.
     * Get|<key>
     * @param source source of the request in the distributed system
     * @param request request
     */
    @Override
    protected void handleGetRequest(String source, String request) {
        addUserRequest(source, request);
    }
    
    /**
     * Handles the increment request from the source.
     * Increment|<key>
     * @param source source of the request in the distributed system
     * @param request request
     */
    @Override
    protected void handleIncrementRequest(String source, String request) {
        addUserRequest(source, request);
    }
    
    /**
     * Handles the decrement request from the source.
     * Decrement|<key>
     * @param source source of the request in the distributed system
     * @param request request
     */
    protected void handleDecrementRequest(String source, String request) {
        addUserRequest(source, request);
    }
    
    /**
     * Handles the vote request from the source.
     * Vote|Get|<key>,<timestamp>,<source>
     * Vote|Increment|<key>,<timestamp>,<source>
     * Vote|Decrement|<key>,<timestamp>,<source>
     * @param source source of the request in the distributed system
     * @param request request
     */
    protected void handleVoteRequest(String source, String request) {
        // There is a corner case.
        // This replica hasn't finished restoration.
        // The primary has updated the membership and hasn't received block request.
        // So this replica should ensures that it is not restoring.
        waitForRestoration();
        String userRequest = request.substring(request.indexOf('|') + 1);
        String response;
        userRequestsLock.readLock().lock();
        try {
            if (userRequests.contains(userRequest) || restoredUserRequests.contains(userRequest)) {
                response = "Yes";
                currentUserRequest = userRequest;
            } else {
                response = "No";
            }
        } finally {
            userRequestsLock.readLock().unlock();
        }
        sendResponse(source, response);
    }
    
    /**
     * Handles the do request from the source.
     * Do|Get|<key>,<timestamp>,<source>
     * Do|Increment|<key>,<timestamp>,<source>
     * Do|Decrement|<key>,<timestamp>,<source>
     * @param source source of the request in the distributed system
     * @param request request
     */
    protected void handleDoRequest(String source, String request) {
        String userRequest = request.substring(request.indexOf('|') + 1);
        userRequestsLock.readLock().lock();
        try {
            if (userRequests.contains(userRequest)) {
                handleUserRequest(userRequest);
            } else {
                handleRestoredUserRequest(userRequest);
            }
        } finally {
            userRequestsLock.readLock().unlock();
        }
        
        userRequestsLock.writeLock().lock();
        try {
            userRequests.remove(userRequest);
        } finally {
            userRequestsLock.writeLock().unlock();
        }
        restoredUserRequests.remove(userRequest);
        previousUserRequest = currentUserRequest;
        currentUserRequest = "";
        sendResponse(source, "ACK");
    }
    
    /**
     * Handles the give up request from the source.
     * GiveUp|Get|<key>,<timestamp>,<source>
     * GiveUp|Increment|<key>,<timestamp>,<source>
     * GiveUp|Decrement|<key>,<timestamp>,<source>
     * @param source source of the request in the distributed system
     * @param request request
     */
    protected void handleGiveUpRequest(String source, String request) {
        currentUserRequest = "";
        sendResponse(source, "ACK");
    }
    
    /**
     * Handles the block request from the source.
     * Block
     * @param source source of the request in the distributed system
     * @param request request
     */
    protected void handleBlockRequest(String source, String request) {
        quiescent = true;
        sendResponse(source, "ACK");
    }
    
    /**
     * Handles the unblock request from the source.
     * Unblock
     * @param source source of the request in the distributed system
     * @param request request
     */
    protected void handleUnblockRequest(String source, String request) {
        quiescent = false;
        if (primary) {
            synchronized(quiescenceObj) {
                quiescenceObj.notify();
            }
        }
        sendResponse(source, "ACK");
    }
    
    /**
     * Handles the restore request from the source.
     * Restore
     * @param source source of the request in the distributed system
     * @param request request
     */
    protected void handleRestoreRequest(String source, String request) {
        StringBuilder sb = new StringBuilder();
        dataLock.readLock().lock();
        try {
            sb.append(checkpoint).append('&');
            sb.append(logBuilder.toString());
        } finally {
            dataLock.readLock().unlock();
        }
        
        if (sb.charAt(sb.length() - 1) == ';') {
            sb.setCharAt(sb.length() - 1, '&');
        } else {
            sb.append('&');
        }
        
        userRequestsLock.readLock().lock();
        try {
            sb.append(serializeUserRequests());
        } finally {
            userRequestsLock.readLock().unlock();
        }
        
        String response = sb.toString();
        sendResponse(source, response);
    }
    
    /**
     * Handles the current request from the source.
     * Current
     * @param source source of the request in the distributed system
     * @param request request
     */
    protected void handleCurrentRequest(String source, String request) {
        sendResponse(source, currentUserRequest);
    }
    
    /**
     * Handles the previous request from the source.
     * Previous
     * @param source source of the request in the distributed system
     * @param request request
     */
    protected void handlePreviousRequest(String source, String request) {
        sendResponse(source, previousUserRequest);
    }
    
    /**
     * Handles user request.
     * Get|<key>,<timestamp>,<source>
     * Increment|<key>,<timestamp>,<source>
     * Decrement|<key>,<timestamp>,<source>
     * @param request request
     */
    private void handleUserRequest(String request) {
        String key = request.substring(request.indexOf('|') + 1, request.indexOf(','));
        String source = request.substring(request.lastIndexOf(',') + 1);
        Integer value = null;
        dataLock.writeLock().lock();
        try {
            switch (getRequestType(request)) {
            case GET:
                value = get(key);
                break;
            case INCREMENT:
                value = increment(key);
                break;
            case DECREMENT:
                value = decrement(key);
                break;
            default:
                printLog(new StringBuilder("Error: Invalid user request ").append(request).append('!').toString());
                System.exit(0);
            }
            logBuilder.append(request).append(';');
        } finally {
            dataLock.writeLock().unlock();
        }
        String response = value == null ? "No such key." : String.valueOf(value);
        sendResponse(source, response);
    }
    
    /**
     * Handles restored user request.
     * Get|<key>,<timestamp>,<source>
     * Increment|<key>,<timestamp>,<source>
     * Decrement|<key>,<timestamp>,<source>
     * @param request request
     */
    private void handleRestoredUserRequest(String request) {
        dataLock.writeLock().lock();
        try {
            reHandleUserRequest(request);
            logBuilder.append(request).append(';');
        } finally {
            dataLock.writeLock().unlock();
        }
    }
    
    /**
     * Re-handles user request.
     * Get|<key>,<timestamp>,<source>
     * Increment|<key>,<timestamp>,<source>
     * Decrement|<key>,<timestamp>,<source>
     * @param request request
     */
    private void reHandleUserRequest(String request) {
        String key = request.substring(request.indexOf('|') + 1, request.indexOf(','));
        switch (getRequestType(request)) {
        case GET:
            break;
        case INCREMENT:
            increment(key);
            break;
        case DECREMENT:
            decrement(key);
            break;
        default:
            printLog(new StringBuilder("Error: Invalid user request ").append(request).append('!').toString());
            System.exit(0);
        }
    }
    
    /**
     * Restores data.
     * @param checkpointResponse checkpoint response from another replica
     * @param logResponse log response from another replica
     */
    private void restoreData(String checkpointResponse, String logResponse) {
        deserializeData(checkpointResponse);
        if (logResponse.length() > 0) {
            String[] requests = logResponse.split(";");
            for (String request : requests) {
                userRequestsLock.readLock().lock();
                try {
                    if (userRequests.contains(request)) {
                        printLog(new StringBuilder("Handle ").append(request).append('.').toString());
                        handleUserRequest(request);
                        userRequests.remove(request);
                    } else {
                        printLog(new StringBuilder("Re-handle ").append(request).append('.').toString());
                        reHandleUserRequest(request);
                    }
                } finally {
                    userRequestsLock.readLock().unlock();
                }
            }
        }
    }
    
    /**
     * Restores user requests
     * @param response response from another replica
     */
    private void restoreUserRequests(String response) {
        if (response.length() > 0) {
            String[] requests = response.split(";");
            userRequestsLock.writeLock().lock();
            try {
                for (String request : requests) {
                    if (!userRequests.contains(request)) {
                        restoredUserRequests.add(request);
                    }
                }
            } finally {
                userRequestsLock.writeLock().unlock();
            }
            synchronized(userRequestsObj) {
                userRequestsObj.notify();
            }
        }
    }
    
    /**
     * Waits for restoration.
     */
    private void waitForRestoration() {
        synchronized(restorationObj) {
            while (!restored) {
                printLog("Wait for restoration.");
                try {
                    restorationObj.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    
    /**
     * Waits for quiescence.
     */
    private void waitForQuiescence() {
        synchronized(quiescenceObj) {
            while (quiescent) {
                printLog("Wait for quiescence.");
                try {
                    quiescenceObj.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    
    /**
     * Waits for user requests.
     */
    private void waitForUserRequests() {
        synchronized(userRequestsObj) {
            while (true) {
                userRequestsLock.readLock().lock();
                try {
                    if (!userRequests.isEmpty()) {
                        break;
                    }
                } finally {
                    userRequestsLock.readLock().unlock();
                }
                printLog("Wait for user requests.");
                try {
                    userRequestsObj.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    
    /**
     * Checkpoint updater.
     *
     */
    private class CheckpointUpdater implements Runnable {
        /**
         * Updates the checkpoint periodically.
         */
        @Override
        public void run() {
            printLog("Launch checkpoint updater.");
            while (true) {
                try {
                    Thread.sleep(checkpointInterval);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                dataLock.writeLock().lock();
                try {
                    checkpoint = serializeData();
                    logBuilder.setLength(0);
                } finally {
                    dataLock.writeLock().unlock();
                }
            }
        }
    }
    
    /**
     * Vote initiator.
     *
     */
    private class VoteInitiator implements Runnable {
        /**
         * Initiates votes.
         */
        @Override
        public void run() {
            printLog("Launch vote initiator.");
            String userRequest, decisionRequest;
            while (true) {
                waitForQuiescence();
                waitForUserRequests();
                
                userRequestsLock.writeLock().lock();
                try {
                    userRequest = userRequests.iterator().next();
                } finally {
                    userRequestsLock.writeLock().unlock();
                }
                
                membershipLock.readLock().lock();
                try {
                    int numFavor = 1;
                    Map<String, String> responses = sendRequestToGroup(membership, name, "Vote|" + userRequest);
                    for (String member : responses.keySet()) {
                        String response = responses.get(member);
                        if (response != null && response.equals("Yes")) {
                            numFavor++;
                        }
                    }
                    
                    if (numFavor == membership.size()) {
                        handleUserRequest(userRequest);
                        userRequestsLock.writeLock().lock();
                        try {
                            userRequests.remove(userRequest);
                        } finally {
                            userRequestsLock.writeLock().unlock();
                        }
                        decisionRequest = "Do|" + userRequest;
                    } else {
                        decisionRequest = "GiveUp|" + userRequest;
                    }
                    sendRequestToGroup(membership, name, decisionRequest);
                } finally {
                    membershipLock.readLock().unlock();
                }
            }
        }
    }
    
    /**
     * Launches an active replica.
     * @param args arguments
     */
    public static void main(String[] args) {
        new ActiveReplica(args[0], args.length >= 2 ? args[1] : null);
    }
}

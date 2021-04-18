import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
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
     * True is this replica finished restoration.
     */
    private boolean restored;
    /**
     * True is this replica is in quiescent state.
     */
    private boolean quiescent;
    /**
     * Lock for checkpoint.
     */
    private final ReentrantReadWriteLock checkpointLock;
    /**
     * Lock for log builder.
     */
    private final ReentrantReadWriteLock logBuilderLock;
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
     * @param logName
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
        this.checkpoint = getCheckpoint();
        this.logBuilder = new StringBuilder();
        this.restored = false;
        this.quiescent = false;
        this.checkpointLock = new ReentrantReadWriteLock();
        this.logBuilderLock = new ReentrantReadWriteLock();
        this.restorationObj = new Object();
        this.quiescenceObj = new Object();
        this.userRequestsObj = new Object();
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
                                int index1 = response.indexOf('|');
                                int index2 = response.indexOf('|', index1 + 1);
                                String checkpointResponse = index1 == 0 ? "" : response.substring(0, index1);
                                String logResponse = index2 == index1 + 1 ? "" : response.substring(index1 + 1, index2);
                                String userRequestsResponse = index2 == response.length() - 1 ? "" : response.substring(index2 + 1);
                                restoreData(checkpointResponse, logResponse);
                                checkpoint = getCheckpoint();
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
                    Map<String, String> responses = sendRequestToGroup(membership, "CurrentUserRequest");
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
                        if (currentUserRequests.size() > 1) {
                            printLog("Error: Replicas have different current user requests!");
                            System.exit(0);
                        }
                        
                        if (freeMembers.isEmpty()) {
                            sendRequestToGroup(busyMembers, "Do," + currentUserRequests.iterator().next());
                        }
                        
                        else {
                            responses = sendRequestToGroup(freeMembers, "PreviousUserRequest");
                            Set<String> previousUserRequests = new HashSet<String>();
                            for (String previousUserRequest : responses.values()) {
                                if (previousUserRequest != null && !previousUserRequest.isEmpty()) {
                                    previousUserRequests.add(previousUserRequest);
                                }
                            }
                            
                            if (!previousUserRequests.isEmpty()) {
                                if (previousUserRequests.size() > 1) {
                                    System.err.println("Error: Replicas have different previous user requests!");
                                    System.exit(0);
                                }
                                
                                if (previousUserRequests.iterator().next().equals(currentUserRequests.iterator().next())) {
                                    sendRequestToGroup(busyMembers, "Do," + currentUserRequests.iterator().next());
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
        
        // Get,<key>
        // Increment,<key>
        // Decrement,<key>
        else if (operation.equals("Get") || operation.equals("Increment") || operation.equals("Decrement")) {
            userRequestsLock.writeLock().lock();
            try {
                userRequests.add(String.join(",", source, request));
            } finally {
                userRequestsLock.writeLock().unlock();
            }
            synchronized(userRequestsObj) {
                userRequestsObj.notify();
            }
        }
        
        // Vote,<request>
        else if (operation.equals("Vote")) {
            waitForRestoration();
            String userRequest = request.substring(request.indexOf(',') + 1);
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
        
        // Do,<request>
        else if (operation.equals("Do")) {
            String userRequest = request.substring(request.indexOf(',') + 1);
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
        
        // GiveUp,<request>
        else if (operation.equals("GiveUp")) {
            sendResponse(source, "ACK");
        }
        
        // Block
        else if (operation.equals("Block")) {
            quiescent = true;
            sendResponse(source, "ACK");
        }
        
        // Unblock
        else if (operation.equals("Unblock")) {
            quiescent = false;
            synchronized(quiescenceObj) {
                quiescenceObj.notify();
            }
            sendResponse(source, "ACK");
        }
        
        // Restore
        else if (operation.equals("Restore")) {
            StringBuilder sb = new StringBuilder();
            checkpointLock.readLock().lock();
            logBuilderLock.readLock().lock();
            try {
                sb.append(checkpoint).append('|');
                sb.append(logBuilder.toString());
            } finally {
                checkpointLock.readLock().unlock();
                logBuilderLock.readLock().unlock();
            }
            
            if (sb.charAt(sb.length() - 1) == ';') {
                sb.setCharAt(sb.length() - 1, '|');
            } else {
                sb.append('|');
            }
            
            userRequestsLock.readLock().lock();
            try {
                for (String userRequest : userRequests) {
                    sb.append(userRequest).append(';');
                }
                if (userRequests.size() > 0) {
                    sb.setLength(sb.length() - 1);
                }
            } finally {
                userRequestsLock.readLock().unlock();
            }
            
            String response = sb.toString();
            sendResponse(source, response);
        }
        
        // CurrentUserRequest
        else if (operation.equals("CurrentUserRequest")) {
            sendResponse(source, currentUserRequest);
        }
        
        // PreviousUserRequest
        else if (operation.equals("PreviousUserRequest")) {
            sendResponse(source, previousUserRequest);
        }
    }
    
    /**
     * Handles user request.
     * @param request request
     */
    private void handleUserRequest(String request) {
        String[] strs = request.split(",");
        String source = strs[0];
        String operation = strs[1];
        String key = strs[2];
        
        // <source>,Get,<key>
        if (operation.equals("Get")) {
            String response;
            dataLock.readLock().lock();
            logBuilderLock.writeLock().lock();
            try {
                if (data.containsKey(key)) {
                    response = String.valueOf(data.get(key));
                } else {
                    response = "No such key.";
                }
                logBuilder.append(request).append(';');
            } finally {
                dataLock.readLock().unlock();
                logBuilderLock.writeLock().unlock();
            }
            sendResponse(source, response);
        }
        
        // <source>,Increment,<key>
        else if (operation.equals("Increment")) {
            String response;
            dataLock.writeLock().lock();
            logBuilderLock.writeLock().lock();
            try {
                if (data.containsKey(key)) {
                    data.put(key, data.get(key) + 1);
                    response = String.valueOf(data.get(key));
                } else {
                    response = "No such key.";
                }
                logBuilder.append(request).append(';');
            } finally {
                dataLock.writeLock().unlock();
                logBuilderLock.writeLock().unlock();
            }
            sendResponse(source, response);
        }
        
        // <source>,Decrement,<key>
        else if (operation.equals("Decrement")) {
            String response;
            dataLock.writeLock().lock();
            logBuilderLock.writeLock().lock();
            try {
                if (data.containsKey(key)) {
                    data.put(key, data.get(key) - 1);
                    response = String.valueOf(data.get(key));
                } else {
                    response = "No such key.";
                }
                logBuilder.append(request).append(';');
            } finally {
                dataLock.writeLock().unlock();
                logBuilderLock.writeLock().unlock();
            }
            sendResponse(source, response);
        }
    }
    
    /**
     * Handles restored user request.
     * @param request request
     */
    private void handleRestoredUserRequest(String request) {
        String[] strs = request.split(",");
        String operation = strs[1];
        String key = strs[2];
        
        // <source>,Get,<key>
        if (operation.equals("Get")) {
            logBuilderLock.writeLock().lock();
            try {
                logBuilder.append(request).append(';');
            } finally {
                logBuilderLock.writeLock().unlock();
            }
        }
        
        // <source>,Increment,<key>
        else if (operation.equals("Increment")) {
            dataLock.writeLock().lock();
            logBuilderLock.writeLock().lock();
            try {
                if (data.containsKey(key)) {
                    data.put(key, data.get(key) + 1);
                }
                logBuilder.append(request).append(';');
            } finally {
                dataLock.writeLock().unlock();
                logBuilderLock.writeLock().unlock();
            }
        }
        
        // <source>,Decrement,<key>
        else if (operation.equals("Decrement")) {
            dataLock.writeLock().lock();
            logBuilderLock.writeLock().lock();
            try {
                if (data.containsKey(key)) {
                    data.put(key, data.get(key) - 1);
                }
                logBuilder.append(request).append(';');
            } finally {
                dataLock.writeLock().unlock();
                logBuilderLock.writeLock().unlock();
            }
        }
    }
    
    /**
     * re-handle user request
     * @param request request
     */
    private void reHandleUserRequest(String request) {
        String[] strs = request.split(",");
        String operation = strs[1];
        String key = strs[2];
        
        // <source>,Increment,<key>
        if (operation.equals("Increment")) {
            if (data.containsKey(key)) {
                data.put(key, data.get(key) + 1);
            }
        }
        
        // <source>,Decrement,<key>
        else if (operation.equals("Decrement")) {
            if (data.containsKey(key)) {
                data.put(key, data.get(key) - 1);
            }
        }
    }
    
    /**
     * Restore data.
     * @param checkpointResponse checkpoint response from another replica
     * @param logResponse log response from another replica
     */
    private void restoreData(String checkpointResponse, String logResponse) {
        if (checkpointResponse.length() > 0) {
            String[] strs = checkpointResponse.split(",");
            data.clear();
            for (int i = 0; i < strs.length; i += 2) {
                String key = strs[i];
                int value = Integer.valueOf(strs[i + 1]);
                data.put(key, value);
            }
        }
        
        if (logResponse.length() > 0) {
            String[] requests = logResponse.split(";");
            for (String request : requests) {
                userRequestsLock.readLock().lock();
                try {
                    if (userRequests.contains(request)) {
                        handleUserRequest(request);
                        userRequests.remove(request);
                    } else {
                        reHandleUserRequest(request);
                    }
                } finally {
                    userRequestsLock.readLock().unlock();
                }
            }
        }
    }
    
    /**
     * Restore user requests
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
     * Wait for restoration.
     */
    private void waitForRestoration() {
        synchronized(restorationObj) {
            while (!restored) {
                try {
                    restorationObj.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    
    /**
     * Wait for quiescence.
     */
    private void waitForQuiescence() {
        synchronized(quiescenceObj) {
            while (quiescent) {
                try {
                    quiescenceObj.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    
    /**
     * Wait for user requests.
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
                checkpointLock.readLock().lock();
                dataLock.readLock().lock();
                logBuilderLock.writeLock().lock();
                try {
                    checkpoint = getCheckpoint();
                    logBuilder.setLength(0);
                } finally {
                    checkpointLock.readLock().unlock();
                    dataLock.readLock().unlock();
                    logBuilderLock.writeLock().unlock();
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
         * Initiate votes.
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
                    for (String member : membership) {
                        if (member.equals(name)) {
                            continue;
                        }
                        String response = sendRequest(member, "Vote," + userRequest);
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
                        decisionRequest = "Do," + userRequest;
                    } else {
                        decisionRequest = "GiveUp," + userRequest;
                    }
                    
                    for (String member : membership) {
                        if (member.equals(name)) {
                            continue;
                        }
                        sendRequest(member, decisionRequest);
                    }
                } finally {
                    membershipLock.readLock().unlock();
                }
            }
        }
    }
    
    private void printUserRequests() {
        StringBuilder sb = new StringBuilder();
        userRequestsLock.readLock().lock();
        try {
            for (String request : userRequests) {
                sb.append(request).append("\n");
            }
        } finally {
            userRequestsLock.readLock().unlock();
        }
        System.out.print(sb.toString());
    }

    public static void main(String[] args) {
        ActiveReplica node = new ActiveReplica(args[0], args.length >= 2 ? args[1] : null);
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println();
            System.out.println("1: get user requests");
            System.out.println("X: kill");
            System.out.println("Please input your operation:");
            String operation = scanner.next();
            
            if (operation.toLowerCase().equals("x")) {
                scanner.close();
                System.exit(0);
            }
            
            if (operation.equals("1")) {
                node.printUserRequests();
            } else {
                System.out.println("Error: Invalid operation!");
            }
        }
    }
}

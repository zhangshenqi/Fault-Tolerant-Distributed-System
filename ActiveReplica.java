import java.util.LinkedHashSet;
import java.util.Scanner;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ActiveReplica extends Replica {
    private LinkedHashSet<String> userRequests;
    private boolean blocked;
    private boolean restored;
    private String checkpoint;
    private StringBuilder logBuilder;
    private final ReentrantReadWriteLock checkpointLock;
    private final ReentrantReadWriteLock logBuilderLock;
    private final Object restorationObj;
    private final Object otherObj;
    private final Object userRequestsObj;
    
    public ActiveReplica(String name) {
        this(name, 1000, 3, 5000, null);
    }
    
    public ActiveReplica(String name, String connectionManagerLogName) {
        this(name, 1000, 3, 5000, connectionManagerLogName);
    }
    
    public ActiveReplica(String name, int heartbeatInterval, int heartbeatTolerance, int checkpointInterval) {
        this(name, heartbeatInterval, heartbeatTolerance, checkpointInterval, null);
    }
    
    public ActiveReplica(String name, int heartbeatInterval, int heartbeatTolerance, int checkpointInterval, String connectionManagerLogName) {
        super(name, heartbeatInterval, heartbeatTolerance, checkpointInterval, connectionManagerLogName);
        userRequests = new LinkedHashSet<String>();
        blocked = false;
        restored = false;
        checkpoint = getCheckpoint();
        logBuilder = new StringBuilder();
        checkpointLock = new ReentrantReadWriteLock();
        logBuilderLock = new ReentrantReadWriteLock();
        restorationObj = new Object();
        otherObj = new Object();
        userRequestsObj = new Object();
    }
    
    @Override
    protected void handleRequest(String source, String request) {
        String[] strs = request.split(",");
        String operation = strs[0];
        
        if (operation.equals("HeartbeatInterval") || operation.equals("HeartbeatTolerance")) {
            super.handleRequest(source, request);
        }
        
        else if (operation.equals("Membership")) {
            membershipLock.writeLock().lock();
            try {
                super.handleRequest(source, request);
                if (!restored) {
                    // If this is not the first replica, restore states and log.
                    if (membership.size() > 1) {
                        for (String member : membership) {
                            if (member.equals(name)) {
                                continue;
                            }
                            sendRequest(member, "Block");
                        }
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
                        for (String member : membership) {
                            if (member.equals(name)) {
                                continue;
                            }
                            sendRequest(member, "Unblock");
                        }
                    }
                    restored = true;
                    synchronized(restorationObj) {
                        restorationObj.notify();
                    }
                    new Thread(new CheckpointUpdater()).start();
                }
                if (!primary && membership.get(0).equals(name)) {
                    primary = true;
                    new Thread(new VoteInitiator()).start();
                }
            } finally {
                membershipLock.writeLock().unlock();
            }
        }
        
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
        
        else if (operation.equals("Vote")) {
            waitForRestoration();
            String userRequest = request.substring(request.indexOf(',') + 1);
            String response;
            userRequestsLock.readLock().lock();
            try {
                if (userRequests.contains(userRequest)) {
                    response = "Yes";
                } else {
                    response = "No";
                }
            } finally {
                userRequestsLock.readLock().unlock();
            }
            sendResponse(source, response);
        }
        
        else if (operation.equals("Do")) {
            sendResponse(source, "ACK");
            String userRequest = request.substring(request.indexOf(',') + 1);
            handleUserRequest(userRequest);
            userRequestsLock.writeLock().lock();
            try {
                userRequests.remove(userRequest);
            } finally {
                userRequestsLock.writeLock().unlock();
            }
        }
        
        else if (operation.equals("GiveUp")) {
            sendResponse(source, "ACK");
        }
        
        else if (operation.equals("Block")) {
            sendResponse(source, "ACK");
            blocked = true;
        }
        
        else if (operation.equals("Unblock")) {
            sendResponse(source, "ACK");
            blocked = false;
            synchronized(otherObj) {
                otherObj.notify();
            }
        }
        
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
    }
    
    private void handleUserRequest(String request) {
        String[] strs = request.split(",");
        String source = strs[0];
        String operation = strs[1];
        
        if (operation.equals("Get")) {
            String key = strs[2];
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
        
        else if (operation.equals("Increment")) {
            String key = strs[2];
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
        
        else if (operation.equals("Decrement")) {
            String key = strs[2];
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
    
    private void reHandleUserRequest(String request) {
        String[] strs = request.split(",");
        String operation = strs[1];
        String key = strs[2];
        
        if (operation.equals("Increment")) {
            if (data.containsKey(key)) {
                data.put(key, data.get(key) + 1);
            }
        }
        
        else if (operation.equals("Decrement")) {
            if (data.containsKey(key)) {
                data.put(key, data.get(key) - 1);
            }
        }
    }
    
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
    
    private void restoreUserRequests(String response) {
        if (response.length() > 0) {
            String[] requests = response.split(";");
            userRequestsLock.writeLock().lock();
            try {
                for (String request : requests) {
                    userRequests.add(request);
                }
            } finally {
                userRequestsLock.writeLock().unlock();
            }
            synchronized(userRequestsObj) {
                userRequestsObj.notify();
            }
        }
    }
    
    private boolean blockedForRestoration() {
        return !restored;
    }
    
    private void waitForRestoration() {
        synchronized(restorationObj) {
            while (blockedForRestoration()) {
                try {
                    restorationObj.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    
    private class CheckpointUpdater implements Runnable {
        @Override
        public void run() {
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
//                    System.out.println("checkpoint: " + checkpoint);
//                    System.out.println("log: " + logBuilder.toString());
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
    
    private class VoteInitiator implements Runnable {
        private boolean blockedForOther() {
            return blocked;
        }
        
        private void waitForOther() {
            synchronized(otherObj) {
                while (blockedForOther()) {
                    try {
                        otherObj.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        
        private boolean blockedForUserRequests() {
            userRequestsLock.readLock().lock();
            try {
                return userRequests.isEmpty();
            } finally {
                userRequestsLock.readLock().unlock();
            }
        }
        
        private void waitForUserRequests() {
            synchronized(userRequestsObj) {
                while (blockedForUserRequests()) {
                    try {
                        userRequestsObj.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        
        @Override
        public void run() {
            String userRequest, decisionRequest;
            while (true) {
                waitForOther();
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

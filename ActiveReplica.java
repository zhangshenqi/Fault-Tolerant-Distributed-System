import java.util.LinkedHashSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ActiveReplica extends Replica {
    private int round;
    private LinkedHashSet<String> userRequests;
    private final ReentrantReadWriteLock userRequestsLock;
    private boolean blocked;
    private final Object blockingObject;
    
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
        super(name, heartbeatInterval, heartbeatTolerance, connectionManagerLogName);
        round = 0;
        userRequests = new LinkedHashSet<String>();
        userRequestsLock = new ReentrantReadWriteLock();
        blocked = false;
        blockingObject = new Object();
    }
    
    @Override
    protected void handleRequest(String source, String request) {
        String[] strs = request.split(",");
        String operation = strs[0];
        
        if (operation.equals("HeartbeatInterval") || operation.equals("HeartbeatTolerance")) {
            super.handleRequest(source, request);
        }
        
        else if (operation.equals("Membership")) {
            boolean start;
            synchronized(membership) {
                start = membership.size() == 0;
            }
            super.handleRequest(source, request);
            synchronized(membership) {
                if (start) {
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
                                int index = response.indexOf('|');
                                
                                // Empty user requests.
                                if (index < 0) {
                                    restoreData(response);
                                }
                                
                                // Empty data.
                                else if (index == 0) {
                                    restoreUserRequests(response);
                                }
                                
                                // Both user requests and data are not empty
                                else {
                                    restoreData(response.substring(0, index));
                                    restoreUserRequests(response.substring(index + 1));
                                }
                                
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
                    new Thread(new VoteInitiator()).start();
                }
            }
            round = 0;
        }
        
        else if (operation.equals("Get") || operation.equals("Increment") || operation.equals("Decrement")) {
            synchronized(userRequests) {
                userRequests.add(String.join(",", source, request));
            }
        }
        
        else if (operation.equals("Vote")) {
            String userRequest = request.substring(request.indexOf(',') + 1);
            String response;
            synchronized(data) {
                if (userRequests.contains(userRequest)) {
                    response = "Yes";
                } else {
                    response = "No";
                }
            }
            sendResponse(source, response);
        }
        
        else if (operation.equals("Do")) {
            sendResponse(source, "ACK");
            String userRequest = request.substring(request.indexOf(',') + 1);
            handleUserRequest(userRequest);
            userRequests.remove(userRequest);
            synchronized(membership) {
                round = (round + 1) % membership.size();
            }
        }
        
        else if (operation.equals("Giveup")) {
            sendResponse(source, "ACK");
            synchronized(membership) {
                round = (round + 1) % membership.size();
            }
        }
        
        else if (operation.equals("Block")) {
            sendResponse(source, "ACK");
            blocked = true;
        }
        
        else if (operation.equals("Unblock")) {
            sendResponse(source, "ACK");
            blocked = false;
            synchronized(blockingObject) {
                blockingObject.notify();
            }
        }
        
        else if (operation.equals("Restore")) {
            StringBuilder sb = new StringBuilder();
            synchronized(data) {
                for (String key : data.keySet()) {
                    sb.append(key).append(',').append(data.get(key)).append(',');
                }
            }
            if (sb.length() > 0) {
                sb.setLength(sb.length() - 1);
            }
            synchronized(userRequests) {
                for (String userRequest : userRequests) {
                    sb.append('|').append(userRequest);
                }
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
            synchronized(data) {
                if (data.containsKey(key)) {
                    response = String.valueOf(data.get(key));
                } else {
                    response = "No such key.";
                }
            }
            sendResponse(source, response);
        }
        
        else if (operation.equals("Increment")) {
            String key = strs[2];
            String response;
            synchronized(data) {
                if (data.containsKey(key)) {
                    data.put(key, data.get(key) + 1);
                    response = "Increment successfully.";
                } else {
                    response = "No such key.";
                }
            }
            sendResponse(source, response);
        }
        
        else if (operation.equals("Decrement")) {
            String key = strs[2];
            String response;
            synchronized(data) {
                if (data.containsKey(key)) {
                    data.put(key, data.get(key) - 1);
                    response = "Decrement successfully.";
                } else {
                    response = "No such key.";
                }
            }
            sendResponse(source, response);
        }
    }
    
    private void restoreData(String response) {
        String[] strs = response.split(",");
        synchronized(data) {
            data.clear();
            for (int i = 0; i < strs.length; i += 2) {
                String key = strs[i];
                int value = Integer.valueOf(strs[i + 1]);
                data.put(key, value);
            }
        }
    }
    
    private void restoreUserRequests(String response) {
        String[] requests = response.split("|");
        for (String request : requests) {
            userRequests.add(request);
        }
    }
    
    private class VoteInitiator implements Runnable {
        public boolean blocked() {
            if (blocked) {
                return true;
            }
            
            membershipLock.readLock().lock();
            try {
                if (round >= membership.size() || !membership.get(round).equals(name)) {
                    return true;
                }
            } finally {
                membershipLock.readLock().unlock();
            }
            
            userRequestsLock.readLock().lock();
            try {
                return userRequests.isEmpty();
            } finally {
                userRequestsLock.readLock().unlock();
            }
        }
        
        @Override
        public void run() {
            String userRequest, decisionRequest;
            while (true) {
                synchronized(blockingObject) {
                    while (blocked == true) {
                        try {
                            blockingObject.wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
                
                synchronized(membership) {
                    if (round >= membership.size() || !membership.get(round).equals(name)) {
                        continue;
                    }
                    
                    synchronized(userRequests) {
                        if (userRequests.isEmpty()) {
                            continue;
                        }
                        userRequest = userRequests.iterator().next();
                    }
                    System.out.println(userRequest);
                    round = (round + 1) % membership.size();
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
                    if (numFavor > membership.size() / 2) {
                        handleUserRequest(userRequest);
                        userRequests.remove(userRequest);
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
                }
            }
        }
    }

    public static void main(String[] args) {
        new ActiveReplica(args[0], args.length >= 2 ? args[1] : null);
    }
}

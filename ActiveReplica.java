import java.util.LinkedHashSet;

public class ActiveReplica extends Replica {
    private boolean blocked;
    private int round;
    private LinkedHashSet<String> userRequests;
    
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
        blocked = true;
        round = 0;
        userRequests = new LinkedHashSet<String>();
    }
    
    @Override
    protected void handleRequest(String source, String request) {
        String[] strs = request.split(",");
        String operation = strs[0];
        
        if (operation.equals("HeartbeatInterval") || operation.equals("HeartbeatTolerance")) {
            super.handleRequest(source, request);
        }
        
        else if (operation.equals("Membership")) {
            boolean start = membership.size() == 0;
            super.handleRequest(source, request);
            // If this is not the first replica, restore states and log.
            if (start) {
                if (membership.size() > 1) {
                    // To do
                }
                blocked = false;
                new Thread(new VoteInitiator()).start();
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
    
    private class VoteInitiator implements Runnable {
        @Override
        public void run() {
            String userRequest, decisionRequest;
            while (true) {
                if (blocked) {
                    continue;
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

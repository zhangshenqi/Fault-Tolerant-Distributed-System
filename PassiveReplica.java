import java.util.concurrent.ConcurrentLinkedQueue;

public class PassiveReplica extends Replica {
    boolean primary;
    private ConcurrentLinkedQueue<String> userRequests;
    int checkpointInterval;
    
    public PassiveReplica(String name) {
        this(name, 1000, 3, 5000, null);
    }
    
    public PassiveReplica(String name, String connectionManagerLogName) {
        this(name, 1000, 3, 5000, connectionManagerLogName);
    }
    
    public PassiveReplica(String name, int heartbeatInterval, int heartbeatTolerance, int checkpointInterval) {
        this(name, heartbeatInterval, heartbeatTolerance, checkpointInterval, null);
    }
    
    public PassiveReplica(String name, int heartbeatInterval, int heartbeatTolerance, int checkpointInterval, String connectionManagerLogName) {
        super(name, heartbeatInterval, heartbeatTolerance, connectionManagerLogName);
        primary = false;
        userRequests = new ConcurrentLinkedQueue<String>();
        this.checkpointInterval = checkpointInterval;
    }
    
    @Override
    protected void handleRequest(String source, String request) {
        String[] strs = request.split(",");
        String operation = strs[0];
        
        if (operation.equals("HeartbeatInterval") || operation.equals("HeartbeatTolerance")) {
            super.handleRequest(source, request);
        }
        
        else if (operation.equals("Membership")) {
            super.handleRequest(source, request);
            if (!primary && membership.get(0).equals(name)) {
                synchronized(data) {
                    primary = true;
                    reHandleUserRequests();
                }
                new Thread(new CheckpointSender()).start();
            }
        }
        
        else if (operation.equals("Checkpoint") && !primary) {
            sendResponse(source, "ACK");
            userRequests.clear();
            synchronized(data) {
                data.clear();
                for (int i = 1; i < strs.length; i += 2) {
                    String key = strs[i];
                    int value = Integer.valueOf(strs[i + 1]);
                    data.put(key, value);
                }
            }
        }
        
        else if (operation.equals("CheckpointInterval")) {
            sendResponse(source, "ACK");
            checkpointInterval = Integer.valueOf(strs[1]);
        }
        
        else if (operation.equals("Get")) {
            if (primary) {
                String key = strs[1];
                int value = data.get(key);
                sendResponse(source, String.valueOf(value));
            } else {
                sendResponse(source, "ACK");
            }
        }
        
        else if (operation.equals("Increment")) {
            if (primary) {
                String key = strs[1];
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
            } else {
                sendResponse(source, "ACK");
                userRequests.add(request);
            }
        }
        
        else if (operation.equals("Decrement")) {
            if (primary) {
                String key = strs[1];
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
            } else {
                sendResponse(source, "ACK");
                userRequests.add(request);
            }
        }
    }
    
    private void reHandleUserRequests() {
        while (!userRequests.isEmpty()) {
            String request = userRequests.poll();
            String[] strs = request.split(",");
            String operation = strs[0];
            String key = strs[1];
            
            if (!data.containsKey(key)) {
                continue;
            }
            
            if (operation.equals("Increment")) {
                data.put(key, data.get(key) + 1);
            }
            
            else if (operation.equals("Decrement")) {
                data.put(key, data.get(key) - 1);
            }
        }
    }
    
    private String getCheckpointRequest() {
        StringBuilder sb = new StringBuilder("Checkpoint");
        synchronized(data) {
            for (String key : data.keySet()) {
                sb.append(',').append(key).append(',').append(data.get(key));
            }
        }
        return sb.toString();
    }
    
    private class CheckpointSender implements Runnable {
        @Override
        public void run() {
            while (true) {
                String request = getCheckpointRequest();
                synchronized(membership) {
                    int size = membership.size();
                    for (int i = 1; i < size; i++) {
                        sendRequest(membership.get(i), request);
                    }
                }
                try {
                    Thread.sleep(checkpointInterval);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        new PassiveReplica(args[0], args.length >= 2 ? args[1] : null);
    }
}

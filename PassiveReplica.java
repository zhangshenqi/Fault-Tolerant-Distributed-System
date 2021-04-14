import java.util.concurrent.ConcurrentLinkedQueue;

public class PassiveReplica extends Replica {
    private ConcurrentLinkedQueue<String> userRequests;
    
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
        super(name, heartbeatInterval, heartbeatTolerance, checkpointInterval, connectionManagerLogName);
        userRequests = new ConcurrentLinkedQueue<String>();
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
            if (!primary) {
                membershipLock.readLock().lock();
                try {
                    if (!membership.get(0).equals(name)) {
                        return;
                    }
                } finally {
                    membershipLock.readLock().unlock();
                }
                reHandleUserRequests();
                primary = true;
                new Thread(new CheckpointSender()).start();
            }
        }
        
        else if (operation.equals("Checkpoint") && !primary) {
            sendResponse(source, "ACK");
            userRequestsLock.writeLock().lock();
            try {
                userRequests.clear();
            } finally {
                userRequestsLock.writeLock().unlock();
            }
            dataLock.writeLock().lock();
            try {
                data.clear();
                for (int i = 1; i < strs.length; i += 2) {
                    String key = strs[i];
                    int value = Integer.valueOf(strs[i + 1]);
                    data.put(key, value);
                }
            } finally {
                dataLock.writeLock().unlock();
            }
        }
        
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
                } finally {
                    dataLock.writeLock().unlock();
                }
                sendResponse(source, response);
            } else {
                sendResponse(source, "ACK");
                userRequestsLock.writeLock().lock();
                try {
                    userRequests.add(request);
                } finally {
                    userRequestsLock.writeLock().unlock();
                }
            }
        }
        
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
                } finally {
                    dataLock.writeLock().unlock();
                }
                sendResponse(source, response);
            } else {
                sendResponse(source, "ACK");
                userRequests.add(request);
            }
        }
    }
    
    private void reHandleUserRequests() {
        userRequestsLock.writeLock().lock();
        dataLock.writeLock().lock();
        try {
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
        } finally {
            userRequestsLock.writeLock().unlock();
            dataLock.writeLock().unlock();
        }
    }
    
    private String getCheckpointRequest() {
        StringBuilder sb = new StringBuilder("Checkpoint");
        dataLock.readLock().lock();
        try {
            for (String key : data.keySet()) {
                sb.append(',').append(key).append(',').append(data.get(key));
            }
        } finally {
            dataLock.readLock().unlock();
        }
        return sb.toString();
    }
    
    private class CheckpointSender implements Runnable {
        @Override
        public void run() {
            while (true) {
                String request = getCheckpointRequest();
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

    public static void main(String[] args) {
        new PassiveReplica(args[0], args.length >= 2 ? args[1] : null);
    }
}

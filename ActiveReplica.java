import java.util.LinkedHashSet;
import java.util.Scanner;

public class ActiveReplica extends Replica {
    private LinkedHashSet<String> userRequests;
    private int round;
    private boolean blocked;
    private final Object restorationObj;
    private final Object roundObj;
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
        super(name, heartbeatInterval, heartbeatTolerance, connectionManagerLogName);
        round = 0;
        userRequests = new LinkedHashSet<String>();
        blocked = false;
        restorationObj = new Object();
        roundObj = new Object();
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
                } else {
                    round = 0;
                    synchronized(roundObj) {
                        roundObj.notify();
                    }
                }
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
            synchronized(membership) {
                round = (round + 1) % membership.size();
            }
            synchronized(roundObj) {
                roundObj.notify();
            }
        }
        
        else if (operation.equals("GiveUp")) {
            sendResponse(source, "ACK");
            synchronized(membership) {
                round = (round + 1) % membership.size();
            }
            synchronized(roundObj) {
                roundObj.notify();
            }
        }
        
        else if (operation.equals("Block")) {
            sendResponse(source, "ACK");
            blocked = true;
        }
        
        else if (operation.equals("Unblock")) {
            sendResponse(source, "ACK");
            blocked = false;
            synchronized(restorationObj) {
                restorationObj.notify();
            }
        }
        
        else if (operation.equals("Restore")) {
            StringBuilder sb = new StringBuilder();
            dataLock.readLock().lock();
            try {
                for (String key : data.keySet()) {
                    sb.append(key).append(',').append(data.get(key)).append(',');
                }
            } finally {
                dataLock.readLock().unlock();
            }
            if (sb.length() > 0) {
                sb.setLength(sb.length() - 1);
            }
            userRequestsLock.readLock().lock();
            try {
                for (String userRequest : userRequests) {
                    sb.append('|').append(userRequest);
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
            try {
                if (data.containsKey(key)) {
                    response = String.valueOf(data.get(key));
                } else {
                    response = "No such key.";
                }
            } finally {
                dataLock.readLock().unlock();
            }
            sendResponse(source, response);
        }
        
        else if (operation.equals("Increment")) {
            String key = strs[2];
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
        }
        
        else if (operation.equals("Decrement")) {
            String key = strs[2];
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
        }
    }
    
    private void restoreData(String response) {
        String[] strs = response.split(",");
        dataLock.writeLock().lock();
        try {
            data.clear();
            for (int i = 0; i < strs.length; i += 2) {
                String key = strs[i];
                int value = Integer.valueOf(strs[i + 1]);
                data.put(key, value);
            }
        } finally {
            dataLock.writeLock().unlock();
        }
    }
    
    private void restoreUserRequests(String response) {
        String[] requests = response.split("|");
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
    
    private class VoteInitiator implements Runnable {
        private boolean blockedForRestoration() {
            return blocked;
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
        
        private boolean blockedForRound() {
            membershipLock.readLock().lock();
            try {
                if (round < membership.size() && membership.get(round).equals(name)) {
                    return false;
                }
                return true;
            } finally {
                membershipLock.readLock().unlock();
            }
        }
        
        private void waitForRound() {
            synchronized(roundObj) {
                while (blockedForRound()) {
                    try {
                        roundObj.wait();
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
                waitForRestoration();
                waitForRound();
                waitForUserRequests();
                
                userRequestsLock.writeLock().lock();
                try {
                    userRequest = userRequests.iterator().next();
                } finally {
                    userRequestsLock.writeLock().unlock();
                }
                
                membershipLock.readLock().lock();
                try {
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
    
    private void printRound() {
        System.out.println(round);
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
            System.out.println("1: get round");
            System.out.println("2: get user requests");
            System.out.println("X: kill");
            System.out.println("Please input your operation:");
            String operation = scanner.next();
            
            if (operation.toLowerCase().equals("x")) {
                scanner.close();
                System.exit(0);
            }
            
            if (operation.equals("1")) {
                node.printRound();
            } else if (operation.equals("2")) {
                node.printUserRequests();
            } else {
                System.out.println("Error: Invalid operation!");
            }
        }
    }
}

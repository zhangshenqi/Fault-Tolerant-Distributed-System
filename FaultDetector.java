import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class FaultDetector extends ConnectionManager {
    private List<String> parents;
    private Map<String, AtomicInteger> childrenTolerance;
    private int heartbeatInterval;
    private int heartbeatTolerance;
    
    public FaultDetector(String name) {
        this(name, 1000, 3, null);
    }
    
    public FaultDetector(String name, String connectionManagerLogName) {
        this(name, 1000, 3, connectionManagerLogName);
    }
    
    public FaultDetector(String name, int heartbeatInterval, int heartbeatTolerance) {
        this(name, heartbeatInterval, heartbeatTolerance, null);
    }
    
    /**
     * Constructs a fault detector with specified name and connection manager log name.
     * @param name name of this sample node.
     * @param logName name of the connection manager log file
     */
    public FaultDetector(String name, int heartbeatInterval, int heartbeatTolerance, String connectionManagerLogName) {
        super(name, true, true, connectionManagerLogName);
        this.heartbeatInterval = heartbeatInterval;
        this.heartbeatTolerance = heartbeatTolerance;
        parents = new ArrayList<String>();
        childrenTolerance = new HashMap<String, AtomicInteger>();
        RandomAccessFile file = null;
        try {
            file = new RandomAccessFile("fault_detector.conf", "r");
            String line = null;
            while ((line = file.readLine()) != null) {
                int index1 = line.indexOf(':');
                if (line.substring(0, index1).equals(name)) {
                    int index2 = line.indexOf(':', index1 + 1);
                    if (index2 > index1 + 1) {
                        parents.addAll(Arrays.asList(line.substring(index1 + 1, index2).split(",")));
                    }
                    if (index2 < line.length() - 1) {
                        String[] children = line.substring(index2 + 1).split(",");
                        for (String child : children) {
                            childrenTolerance.put(child, new AtomicInteger(0));
                        }
                    }
                    break;
                }
            }
        }  catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                file.close();
            }  catch (IOException e) {
                e.printStackTrace();
            }
        }
        
        if (parents.size() > 0) {
            new Thread(new HeartbeatSender()).start();
        }
        
        if (childrenTolerance.size() > 0) {
            new Thread(new ToleranceDecrementer()).start();
        }
    }
    
    @Override
    protected void handleRequest(String source, String request) {
        String[] strs = request.split(",");
        String operation = strs[0];
        
        if (operation.equals("Alive")) {
            System.out.println(strs[1] + " is alive.");
            sendResponse(source, "ACK");
            sendRequestsToParents(request);
        }
        
        else if (operation.equals("Dead")) {
            System.out.println(strs[1] + " is dead.");
            sendResponse(source, "ACK");
            sendRequestsToParents(request);
        }
        
        else if (operation.equals("HeartbeatInterval")) {
            sendResponse(source, "ACK");
            setHeartbeatInterval(Integer.valueOf(strs[1]));
            sendRequestsToChildren(request);
        }
        
        else if (operation.equals("HeartbeatTolerance")) {
            sendResponse(source, "ACK");
            setHeartbeatTolerance(Integer.valueOf(strs[1]));
            sendRequestsToChildren(request);
        }
    }

    @Override
    protected void handleMessage(String source, String message) {
        String[] strs = message.split(",");
        String operation = strs[0];
        if (operation.equals("Heartbeat")) {
            if (childrenTolerance.containsKey(source)) {
                AtomicInteger childTolerance = childrenTolerance.get(source);
                if (childTolerance.getAndSet(heartbeatTolerance) <= 0) {
                    System.out.println(source + " is alive.");
                    sendRequestsToParents("Alive," + source);
                }
            }
        }
    }
    
    protected void sendRequestsToParents(String request) {
        for (String parent : parents) {
            sendRequest(parent, request);
        }
    }
    
    protected void sendRequestsToChildren(String request) {
        for (String child : childrenTolerance.keySet()) {
            sendRequest(child, request);
        }
    }
    
    protected void setHeartbeatInterval(int heartbeatInterval) {
        if (heartbeatInterval > 0) {
            this.heartbeatInterval = heartbeatInterval;
        }
    }
    
    protected void setHeartbeatTolerance(int heartbeatTolerance) {
        if (heartbeatTolerance > 0) {
            this.heartbeatTolerance = heartbeatTolerance;
        }
    }
    
    private class HeartbeatSender implements Runnable {
        @Override
        public void run() {
            while (true) {
                for (String parent : parents) {
                    sendMessage(parent, "Heartbeat");
                }
                try {
                    Thread.sleep(heartbeatInterval);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    
    private class ToleranceDecrementer implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep(heartbeatInterval);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                for (String child : childrenTolerance.keySet()) {
                    AtomicInteger childTolerance = childrenTolerance.get(child);
                    if (childTolerance.get() > 0) {
                        int updatedChildTolerance = childTolerance.decrementAndGet();
                        if (updatedChildTolerance == 0) {
                            System.out.println(child + " is dead.");
                            sendRequestsToParents("Dead," + child);
                        }
                    }
                }
            }
        }
    }
    
    /**
     * A shell which allow users to test the fault detector.
     * @param args arguments
     */
    public static void main(String[] args) {
        new FaultDetector(args[0], args.length >= 2 ? args[1] : null);
    }
}

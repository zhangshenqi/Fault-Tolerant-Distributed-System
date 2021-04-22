import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A fault detector which sends heartbeats to parents and receives heartbeats from children.
 * @author Shenqi Zhang
 *
 */
public class FaultDetector extends ConnectionManager {
    /**
     * Default heartbeat interval.
     */
    protected static final int DEFAULT_HEARTBEAT_INTERVAL = 200;
    /**
     * Default heartbeat tolerance.
     */
    protected static final int DEFAULT_HEARTBEAT_TOLERANCE = 3;
    /**
     * Parents of this node.
     */
    private List<String> parents;
    /**
     * A map with children as keys and remaining tolerance as values.
     */
    private Map<String, AtomicInteger> childrenTolerance;
    /**
     * Heartbeat interval.
     */
    protected int heartbeatInterval;
    /**
     * Heartbeat tolerance.
     * When the number of consecutive missing heartbeats reaches the tolerance, a child is considered dead.
     */
    private int heartbeatTolerance;
    
    /**
     * Constructs a fault detector.
     * @param name the name of this node in the distributed system
     */
    public FaultDetector(String name) {
        this(name, DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_HEARTBEAT_TOLERANCE, null);
    }
    
    /**
     * Constructs a fault detector.
     * @param name the name of this node in the distributed system
     * @param logName the name of the log file; if null, log will be written in stdout
     */
    public FaultDetector(String name, String logName) {
        this(name, DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_HEARTBEAT_TOLERANCE, logName);
    }
    
    /**
     * Constructs a fault detector.
     * @param name the name of this node in the distributed system
     * @param heartbeatInterval heartbeat interval
     * @param heartbeatTolerance heartbeat tolerance
     */
    public FaultDetector(String name, int heartbeatInterval, int heartbeatTolerance) {
        this(name, heartbeatInterval, heartbeatTolerance, null);
    }
    
    /**
     * Constructs a fault detector.
     * @param name the name of this node in the distributed system
     * @param heartbeatInterval heartbeat interval
     * @param heartbeatTolerance heartbeat tolerance
     * @param logName the name of the log file; if null, log will be written in stdout
     */
    public FaultDetector(String name, int heartbeatInterval, int heartbeatTolerance, String logName) {
        super(name, true, true, logName);
        Map<String, String> parameters = getParameters("fault_detector.conf");
        if (heartbeatInterval > 0) {
            this.heartbeatInterval = heartbeatInterval;
        } else {
            this.heartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL;
        }
        if (heartbeatTolerance > 0) {
            this.heartbeatTolerance = heartbeatTolerance;
        } else {
            this.heartbeatTolerance = DEFAULT_HEARTBEAT_TOLERANCE;
        }
        String str = parameters.get(name);
        int index = str.indexOf('|');
        this.parents = new ArrayList<String>();
        if (index > 0) {
            this.parents.addAll(Arrays.asList(str.substring(0, index).trim().split("\\s*,\\s*")));
        }
        this.childrenTolerance = new HashMap<String, AtomicInteger>();
        if (index < str.length() - 1) {
            for (String child : str.substring(index + 1).split(",")) {
                this.childrenTolerance.put(child.trim(), new AtomicInteger(0));
            }
        }
        
        printParameters();
        
        if (parents.size() > 0) {
            new Thread(new HeartbeatSender()).start();
        }
        
        if (childrenTolerance.size() > 0) {
            new Thread(new ToleranceDecrementer()).start();
        }
    }
    
    /**
     * Sets heartbeat interval.
     * @param heartbeatInterval heartbeat interval
     */
    protected void setHeartbeatInterval(int heartbeatInterval) {
        if (heartbeatInterval > 0) {
            this.heartbeatInterval = heartbeatInterval;
        }
        printLog("heartbeat interval = " + this.heartbeatInterval);
    }
    
    /**
     * Sets heartbeat tolerance.
     * @param heartbeatTolerance heartbeat tolerance
     */
    protected void setHeartbeatTolerance(int heartbeatTolerance) {
        if (heartbeatTolerance > 0) {
            this.heartbeatTolerance = heartbeatTolerance;
        }
        printLog("heartbeat tolerance = " + this.heartbeatTolerance);
    }
    
    /**
     * Sends request to parents concurrently.
     * @param request request
     */
    protected void sendRequestToParents(String request) {
        sendRequestToGroup(parents, request);
    }
    
    /**
     * Sends request to children concurrently.
     * @param request request
     */
    protected void sendRequestToChildren(String request) {
        sendRequestToGroup(childrenTolerance.keySet(), request);
    }
    
    /**
     * Handles the request from the source.
     * @param source source of the request in the distributed system
     * @param request request
     */
    @Override
    protected void handleRequest(String source, String request) {
        switch (getRequestType(request)) {
        case ALIVE:
            handleAliveRequest(source, request);
            break;
        case DEAD:
            handleDeadRequest(source, request);
            break;
        case HEARTBEAT_INTERVAL:
            handleHeartbeatIntervalRequest(source, request);
            break;
        case HEARTBEAT_TOLERANCE:
            handleHeartbeatToleranceRequest(source, request);
            break;
        default:
            printLog(new StringBuilder("Error: Invalid request ").append(request).append('!').toString());
            System.exit(0);
        }
    }
    
    /**
     * Handles the alive request from the source.
     * Alive|<node>
     * @param source source of the request in the distributed system
     * @param request request
     */
    protected void handleAliveRequest(String source, String request) {
        String node = request.substring(request.indexOf('|') + 1);
        printLog(node + " is alive.");
        sendRequestToParents(request);
        sendResponse(source, "ACK");
    }
    
    /**
     * Handles the dead request from the source.
     * Dead|<node>
     * @param source source of the request in the distributed system
     * @param request request
     */
    protected void handleDeadRequest(String source, String request) {
        String node = request.substring(request.indexOf('|') + 1);
        printLog(node + " is dead.");
        sendRequestToParents(request);
        sendResponse(source, "ACK");
    }
    
    /**
     * Handles the heartbeat interval request from the source.
     * HeartbeatInterval|<interval>
     * @param source source of the request in the distributed system
     * @param request request
     */
    protected void handleHeartbeatIntervalRequest(String source, String request) {
        // If heartbeat interval decreases, set children's interval first.
        // If heartbeat interval increases, set the interval of this node first.
        int interval = Integer.valueOf(request.substring(request.indexOf('|') + 1));
        if (interval < heartbeatInterval) {
            sendRequestToChildren(request);
            setHeartbeatInterval(interval);
        } else {
            setHeartbeatInterval(interval);
            sendRequestToChildren(request);
        }
        sendResponse(source, "ACK");
    }
    
    /**
     * Handles the heartbeat tolerance request from the source.
     * HeartbeatTolerance|<tolerance>
     * @param source source of the request in the distributed system
     * @param request request
     */
    protected void handleHeartbeatToleranceRequest(String source, String request) {
        int tolerance = Integer.valueOf(request.substring(request.indexOf('|') + 1));
        setHeartbeatTolerance(tolerance);
        sendRequestToChildren(request);
        sendResponse(source, "ACK");
    }
    
    /**
     * Handles the message from the source.
     * @param source the source in the distributed system
     * @param message message
     */
    @Override
    protected void handleMessage(String source, String message) {
        switch (getMessageType(message)) {
        case HEARTBEAT:
            handleHeartbeatMessage(source, message);
            break;
        default:
            printLog(new StringBuilder("Error: Invalid message ").append(message).append('!').toString());
            System.exit(0);
        }
    }
    
    /**
     * Handles the heartbeat message from the source.
     * Heartbeat
     * @param source the source in the distributed system
     * @param message message
     */
    protected void handleHeartbeatMessage(String source, String message) {
        if (childrenTolerance.containsKey(source)) {
            AtomicInteger childTolerance = childrenTolerance.get(source);
            if (childTolerance.getAndSet(heartbeatTolerance) <= 0) {
                printLog(source + " is alive.");
                sendRequestToParents("Alive|" + source);
            }
        }
    }
    
    /**
     * Prints the parameters.
     */
    private void printParameters() {
        StringBuilder sb = new StringBuilder();
        sb.append("parents = ");
        if (!this.parents.isEmpty()) {
            for (String parent : this.parents) {
                sb.append(parent).append(", ");
            }
            sb.setLength(sb.length() - 2);
        }
        sb.append('\n');
        sb.append("children = ");
        if (!this.childrenTolerance.isEmpty()) {
            for (String child : this.childrenTolerance.keySet()) {
                sb.append(child).append(", ");
            }
            sb.setLength(sb.length() - 2);
        }
        sb.append('\n');
        sb.append("heartbeat interval = ").append(this.heartbeatInterval).append('\n');
        sb.append("heartbeat tolerance = ").append(this.heartbeatTolerance);
        printLog(sb.toString());
    }
    
    /**
     * Heartbeat sender.
     *
     */
    private class HeartbeatSender implements Runnable {
        /**
         * Sends heartbeat to parents periodically.
         */
        @Override
        public void run() {
            printLog("Launch heartbeat sender.");
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
    
    /**
     * Tolerance decrementer.
     *
     */
    private class ToleranceDecrementer implements Runnable {
        /**
         * Decrements the tolerance of each child periodically.
         */
        @Override
        public void run() {
            printLog("Launch tolerance decrementer.");
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
                            printLog(child + " is dead.");
                            sendRequestToParents("Dead|" + child);
                        }
                    }
                }
            }
        }
    }
    
    /**
     * Launches a fault detector.
     * @param args arguments
     */
    public static void main(String[] args) {
        new FaultDetector(args[0], args.length >= 2 ? args[1] : null);
    }
}

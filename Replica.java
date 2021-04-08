import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Replica extends FaultDetector {
    protected String replicaManager;
    protected List<String> membership;
    protected Map<String, Integer> data;
    protected final ReentrantReadWriteLock membershipLock;
    
    public Replica(String name) {
        this(name, 1000, 3, null);
    }
    
    public Replica(String name, String connectionManagerLogName) {
        this(name, 1000, 3, connectionManagerLogName);
    }
    
    public Replica(String name, int heartbeatInterval, int heartbeatTolerance) {
        this(name, heartbeatInterval, heartbeatTolerance, null);
    }
    
    /**
     * Constructs a fault detector with specified name and connection manager log name.
     * @param name name of this sample node.
     * @param logName name of the connection manager log file
     */
    public Replica(String name, int heartbeatInterval, int heartbeatTolerance, String connectionManagerLogName) {
        super(name, heartbeatInterval, heartbeatTolerance, connectionManagerLogName);
        data = new HashMap<String, Integer>();
        RandomAccessFile file = null;
        try {
            file = new RandomAccessFile("replica.conf", "r");
            replicaManager = file.readLine();
            String line = null;
            while ((line = file.readLine()) != null) {
                int index = line.indexOf(':');
                String key = line.substring(0, index);
                int value = Integer.valueOf(line.substring(index + 1));
                data.put(key, value);
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
        membership = new ArrayList<String>();
        membershipLock = new ReentrantReadWriteLock();
    }
    
    @Override
    protected void handleRequest(String source, String request) {
        String[] strs = request.split(",");
        String operation = strs[0];
        
        if (operation.equals("HeartbeatInterval") || operation.equals("HeartbeatTolerance")) {
            super.handleRequest(source, request);
        }
        
        else if (operation.equals("Membership")) {
            sendResponse(source, "ACK");
            synchronized (membership) {
                membership.clear();
                for (int i = 1; i < strs.length; i++) {
                    membership.add(strs[i]);
                }
            }
        }
    }
    
    public static void main(String[] args) {
        new Replica(args[0], args.length >= 2 ? args[1] : null);
    }
}

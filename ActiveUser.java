import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A user which sends requests to replicas performing active replication.
 * @author Shenqi Zhang
 *
 */
public class ActiveUser extends User {
    /**
     * Constructs an active user.
     * @param name the name of this node in the distributed system
     */
    public ActiveUser(String name) {
        this(name, null);
    }
    
    /**
     * Constructs an active user.
     * @param name the name of this node in the distributed system
     * @param logName the name of the log file; if null, log will be written in stdout
     */
    public ActiveUser(String name, String logName) {
        super(name, logName);
    }
    
    /**
     * Sends request to replicas.
     * @param request request
     * @return response
     */
    @Override
    protected String sendRequestToReplicas(String request) {
        String membership = sendRequest(replicaManager, "Membership");
        if (membership.length() == 0) {
            return "Error: No server is available!";
        }
        
        Map<String, String> responses = sendRequestToGroup(Arrays.asList(membership.split(",")), request);
        Set<String> set = new HashSet<String>();
        for (String response : responses.values()) {
            if (response != null) {
                set.add(response);
            }
        }
        
        if (set.size() > 1) {
            printLog("Error: Responses are different!");
            System.exit(0);
        }
        
        if (set.size() == 0) {
            return "Error: No server is not available!";
        }
        
        return set.iterator().next();
    }
    
    /**
     * Launches an active user and tests it.
     * @param args arguments
     */
    public static void main(String[] args) {
        ActiveUser node = new ActiveUser(args[0], args.length >= 2 ? args[1] : null);
        test(node);
    }
}

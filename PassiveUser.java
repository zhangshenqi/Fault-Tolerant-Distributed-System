import java.util.Arrays;

/**
 * A user which sends requests to replicas performing passive replication.
 * @author Shenqi Zhang
 *
 */
public class PassiveUser extends User {
    /**
     * Constructs a passive user.
     * @param name the name of this node in the distributed system
     */
    public PassiveUser(String name) {
        this(name, null);
    }
    
    /**
     * Constructs a passive user.
     * @param name the name of this node in the distributed system
     * @param logName the name of the log file; if null, log will be written in stdout
     */
    public PassiveUser(String name, String logName) {
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
        
        String[] members = membership.split(",");
        String response = sendRequest(members[0], request);
        if (response == null) {
            response = "Error: No server is available!";
        } else {
            sendRequestToGroup(Arrays.asList(members), members[0], request);
        }
        return response;
    }
    
    /**
     * Launches a passive user and tests it.
     * @param args arguments
     */
    public static void main(String[] args) {
        PassiveUser node = new PassiveUser(args[0], args.length >= 2 ? args[1] : null);
        test(node);
    }
}

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ActiveUser extends User {
    public ActiveUser(String name) {
        this(name, null);
    }
    
    public ActiveUser(String name, String connectionManagerLogName) {
        super(name, connectionManagerLogName);
    }

    @Override
    protected String sendRequestToReplicas(String request) {
        String membership = sendRequest(replicaManager, "Membership");
        if (membership.length() == 0) {
            return "Error: Server is not available!";
        }
        
        Map<String, String> responses = sendRequestToGroup(Arrays.asList(membership.split(",")), request);
        Set<String> set = new HashSet<String>();
        for (String response : responses.values()) {
            if (responses != null) {
                set.add(response);
            }
        }
        
        if (set.size() > 1) {
            System.out.println("Error: Responses are different!");
            System.exit(0);
        }
        
        if (set.size() == 0) {
            return "Error: Server is not available!";
        } else {
            return set.iterator().next();
        }
    }
    
    public static void main(String[] args) {
        ActiveUser node = new ActiveUser(args[0], args.length >= 2 ? args[1] : null);
        test(node);
    }
}

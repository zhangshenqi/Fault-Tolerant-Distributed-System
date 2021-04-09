
public class PassiveUser extends User {
    public PassiveUser(String name) {
        this(name, null);
    }
    
    public PassiveUser(String name, String connectionManagerLogName) {
        super(name, connectionManagerLogName);
    }

    @Override
    protected String sendRequestToReplicas(String request) {
        String membership = sendRequest(replicaManager, "Membership");
        if (membership.length() == 0) {
            return "Error: Server is not available!";
        }
        
        String[] members = membership.split(",");
        String response = sendRequest(members[0], request);
        if (response == null) {
            response = "Error: Server is not available!";
        } else {
            for (int i = 1; i < members.length; i++) {
                sendRequest(members[i], request);
            }
        }
        return response;
    }

    public static void main(String[] args) {
        PassiveUser node = new PassiveUser(args[0], args.length >= 2 ? args[1] : null);
        test(node);
    }
}

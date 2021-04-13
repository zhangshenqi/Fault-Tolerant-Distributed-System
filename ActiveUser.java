import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
        
        Set<String> responses = new HashSet<String>();
        String[] members = membership.split(",");
        List<Thread> threads = new ArrayList<Thread>(members.length);
        for (String member : members) {
            Thread thread = new Thread(new RequestSender(responses, member, request));
            threads.add(thread);
            thread.start();
        }
        
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        
        if (responses.size() > 1) {
            System.out.println("Error: Responses are different!");
            System.exit(0);
        }
        
        if (responses.size() == 0) {
            return "Error: Server is not available!";
        } else {
            return responses.iterator().next();
        }
    }
    
    private class RequestSender implements Runnable {
        Set<String> responses;
        String destination;
        String request;
        
        RequestSender(Set<String> responses, String destination, String request) {
            this.responses = responses;
            this.destination = destination;
            this.request = request;
        }
        
        @Override
        public void run() {
            String response = sendRequest(destination, request);
            if (response != null) {
                synchronized(responses) {
                    responses.add(response);
                }
            }
        }
    }
    
    public static void main(String[] args) {
        ActiveUser node = new ActiveUser(args[0], args.length >= 2 ? args[1] : null);
        test(node);
    }
}

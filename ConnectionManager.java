import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public abstract class ConnectionManager {
    
    private enum OPERATION {SEND_REQUEST, RECEIVE_REQUEST,
                            SEND_RESPONSE, RECEIVE_RESPONSE,
                            SEND_MESSAGE, RECEIVE_MESSAGE};
    
    private String name;
    private Map<String, Peer> peers;
    
    public ConnectionManager(String name, boolean launchTCPServer) {
        this.name = name;
        peers = new HashMap<String, Peer>();
        RandomAccessFile file = null;
        try {
            file = new RandomAccessFile("peers.conf", "r");
            String line = null;
            while ((line = file.readLine()) != null) {
                String[] strs = line.split(":");
                Peer peer = new Peer(strs[0], strs[1], Integer.valueOf(strs[2]));
                peers.put(strs[0], peer);
            }
        }  catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                file.close();
            }  catch (Exception e) {
                e.printStackTrace();
            }
        }
        
        if (launchTCPServer) {
            new Thread(new TCPServer(peers.get(name).backendPort)).start();
        }
    }
    
    protected abstract void handleRequest(String source, String request);
    
    private void printLog(OPERATION operation, String host, String text) {
        StringBuilder sb = new StringBuilder(new SimpleDateFormat("HH:mm:ss.SSS").format(new Date()));
        sb.append(' ').append(name);
        switch (operation) {
        case SEND_REQUEST:
            sb.append(" sends request to ");
            break;
        case RECEIVE_REQUEST:
            sb.append(" receives request from ");
            break;
        case SEND_RESPONSE:
            sb.append(" sends response to ");
            break;
        case RECEIVE_RESPONSE:
            sb.append(" receives response from ");
            break;
        case SEND_MESSAGE:
            sb.append(" sends message to ");
            break;
        case RECEIVE_MESSAGE:
            sb.append(" receives message from ");
            break;
        }
        sb.append(host).append(": ").append(text);
        System.out.println(sb.toString());
    }
    
    private void printLog(String text) {
        System.out.println(text);
    }
    
    protected String sendRequest(String destination, String request) {
        printLog(OPERATION.SEND_REQUEST, destination, request);
        
        if (!peers.containsKey(destination)) {
            printLog("Error: No such destination!");
            return null;
        }
        
        request = (new StringBuilder(name)).append(',').append(request).toString();
        Peer peer = peers.get(destination);
        return sendRequest(peer, request, peer.clientSocket == null);
    }
    
    private String sendRequest(Peer peer, String request, boolean rebuildConnection) {
        if (rebuildConnection) {
            try {
                peer.clientSocket = new Socket(peer.address, peer.backendPort);
                peer.clientReader = new BufferedReader(new InputStreamReader(peer.clientSocket.getInputStream()));
                peer.clientWriter = new PrintWriter(peer.clientSocket.getOutputStream(), true);
            } catch (IOException e) {
                printLog("Error: Connection fails!");
                return null;
            }
        }
        
        peer.clientWriter.println(request);
        String response = null;
        try {
            response = peer.clientReader.readLine();
        } catch (IOException e) {}
        
        if (response == null) {
            if (rebuildConnection) {
                peer.clientSocket = null;
                peer.clientReader = null;
                peer.clientWriter = null;
                printLog("Error: Connection fails!");
                return null;
            } else {
                return sendRequest(peer, request, true);
            }
        } else {
            printLog(OPERATION.RECEIVE_RESPONSE, peer.name, response);
        }
        
        return response;
    }
    
    protected void sendResponse(String source, String response) {
        printLog(OPERATION.SEND_RESPONSE, source, response);
        
        if (!peers.containsKey(source)) {
            printLog("Error: No such source!");
        }
        
        Peer peer = peers.get(source);
        if (peer.serverWriter == null) {
            printLog("Error: No such connection!");
        }
        
        peer.serverWriter.println(response);
    }
    
    private class TCPServer implements Runnable {
        private int backendPort;
        
        TCPServer(int backendPort) {
            this.backendPort = backendPort;
        }
        
        @Override
        public void run() {
            ServerSocket serverSocket = null;
            try {
                serverSocket = new ServerSocket(backendPort);
                while (true) {
                    Socket socket = serverSocket.accept();
                    new Thread(new TCPClientHandler(socket)).start();
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            
        }
    }
    
    private class TCPClientHandler implements Runnable {
        private Socket socket;
        
        TCPClientHandler(Socket socket) {
            this.socket = socket;
        }
        
        @Override
        public void run() {
            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                String request = null, source = null;
                boolean firstRequest = true;
                while ((request = reader.readLine()) != null) {
                    int index = request.indexOf(',');
                    if (firstRequest) {
                        source = request.substring(0, index);
                        peers.get(source).serverWriter = writer;
                        firstRequest = false;
                    }
                    request = request.substring(index + 1);
                    printLog(OPERATION.RECEIVE_REQUEST, source, request);
                    handleRequest(source, request);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    
    private class Peer {
        private String name;
        private String address;
        private int backendPort;
        private Socket clientSocket;
        private BufferedReader clientReader;
        private PrintWriter clientWriter;
        private PrintWriter serverWriter;
        
        Peer(String name, String address, int backendPort) {
            this.name  = name;
            this.address = address;
            this.backendPort = backendPort;
        }
    }
    
}

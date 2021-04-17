import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A connection manager which provides network methods for nodes in distributed systems.
 * @author Shenqi Zhang
 *
 */
public abstract class ConnectionManager {
    /**
     * Operations provided in this connection manager.
     */
    private enum OPERATION {SEND_REQUEST, RECEIVE_REQUEST,
                            SEND_RESPONSE, RECEIVE_RESPONSE,
                            SEND_MESSAGE, RECEIVE_MESSAGE};
    /**
     * Size of the packet data when receiving message.
     */
    private static final int BUF_SIZE = 100;
    /**
     * Name of this node in the distributed system.
     */
    protected String name;
    /**
     * Peer nodes in the distributed system.
     */
    private Map<String, Peer> peers;
    /**
     * The socket to send and receive UDP messages.
     */
    private DatagramSocket datagramSocket;
    /**
     * The log writer.
     */
    private PrintStream logWriter;
    
    /**
     * Constructs a connection manager.
     * @param name the name of this node in the distributed system
     * @param launchTCPServer true if a TCP server will be launched
     * @param launchUDPServer true if a UDP server will be launched
     */
    public ConnectionManager(String name, boolean launchTCPServer, boolean launchUDPServer) {
        this(name, launchTCPServer, launchUDPServer, null);
    }
    
    /**
     * Constructs a connection manager.
     * @param name the name of this node in the distributed system
     * @param launchTCPServer true if a TCP server will be launched
     * @param launchUDPServer true if a UDP server will be launched
     * @param logName the name of the log file; if null, log will be written in stdout
     */
    public ConnectionManager(String name, boolean launchTCPServer, boolean launchUDPServer, String logName) {
        Map<String, String> parameters = getParameters("connection_manager.conf");
        this.name = name;
        this.peers = new HashMap<String, Peer>(parameters.size());
        for (String peerName : parameters.keySet()) {
            String str = parameters.get(peerName);
            int index = str.indexOf(':');
            String peerAddress = str.substring(0, index).trim();
            int peerBackendPort = Integer.valueOf(str.substring(index + 1).trim());
            Peer peer = new Peer(peerName, peerAddress, peerBackendPort);
            this.peers.put(peerName, peer);
        }
        try {
            this.datagramSocket = new DatagramSocket(peers.get(name).backendPort);
        } catch (SocketException e) {
            e.printStackTrace();
        }
        if (logName == null) {
            this.logWriter = System.out;
        } else {
            try {
                this.logWriter = new PrintStream(new FileOutputStream(logName));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        
        printParameters();
        
        if (launchTCPServer) {
            new Thread(new TCPServer(peers.get(name).backendPort)).start();
        }
        
        if (launchUDPServer) {
            new Thread(new UDPServer()).start();
        }
    }
    
    /**
     * Sends request to the specified destination in the distributed system.
     * @param destination the specified destination in the distributed system
     * @param request request
     * @return response from the destination, or null if fails
     */
    protected String sendRequest(String destination, String request) {
        printLog(OPERATION.SEND_REQUEST, destination, request);
        
        if (!peers.containsKey(destination)) {
            printLog("Error: No such destination!");
            return null;
        }
        
        request = new StringBuilder(name).append(',').append(request).toString();
        Peer peer = peers.get(destination);
        return sendRequest(peer, request, peer.clientSocket == null);
    }
    
    /**
     * Sends request to the specified peer node in the distributed system.
     * @param peer the specified peer node
     * @param request request
     * @param rebuildConnection true if a new TCP connection will be built
     * @return response from the destination, or null if fails
     */
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
    
    /**
     * Sends request to a group of destinations in the distributed system concurrently.
     * @param group group of destinations
     * @param request request
     * @return a map with destinations as keys and responses as values.
     */
    protected Map<String, String> sendRequestToGroup(Collection<String> group, String request) {
        Map<String, String> responses = new HashMap<String, String>();
        List<Thread> threads = new ArrayList<Thread>(group.size());
        for (String destination : group) {
            threads.add(new Thread(new RequestSender(destination, request, responses)));
        }
        startAndJoinThreads(threads);
        return responses;
    }
    
    /**
     * Sends request to a group of destinations in the distributed system concurrently.
     * @param group group of destinations
     * @param exception destination excepted
     * @param request request
     * @return a map with destinations as keys and responses as values
     */
    protected Map<String, String> sendRequestToGroup(Collection<String> group, String exception, String request) {
        Map<String, String> responses = new HashMap<String, String>();
        List<Thread> threads = new ArrayList<Thread>(group.size());
        for (String destination : group) {
            if (!destination.equals(exception)) {
                threads.add(new Thread(new RequestSender(destination, request, responses)));
            }
        }
        startAndJoinThreads(threads);
        return responses;
    }
    
    /**
     * Starts and joins threads.
     * @param threads threads
     */
    private void startAndJoinThreads(List<Thread> threads) {
        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    
    /**
     * Handles the request from the source.
     * @param source source of the request in the distributed system
     * @param request request
     */
    protected abstract void handleRequest(String source, String request);
    
    /**
     * Sends response to the source in the distributed system.
     * @param source source in the distributed system
     * @param response response
     */
    protected void sendResponse(String source, String response) {
        printLog(OPERATION.SEND_RESPONSE, source, response);
        
        if (!peers.containsKey(source)) {
            printLog("Error: No such source!");
            return;
        }
        
        Peer peer = peers.get(source);
        if (peer.serverWriter == null) {
            printLog("Error: No such connection!");
            return;
        }
        
        peer.serverWriter.println(response);
    }
    
    /**
     * Sends message to the specified destination in the distributed system.
     * @param destination the specified destination in the distributed system
     * @param message message
     */
    protected void sendMessage(String destination, String message) {
        printLog(OPERATION.SEND_MESSAGE, destination, message);
        
        if (!peers.containsKey(destination)) {
            printLog("Error: No such destination!");
            return;
        }
        
        message = new StringBuilder(name).append(',').append(message).toString();
        Peer peer = peers.get(destination);
        byte[] buf = message.getBytes();
        try {
            DatagramPacket packet = new DatagramPacket(buf, buf.length, InetAddress.getByName(peer.address), peer.backendPort);
            datagramSocket.send(packet);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    /**
     * Handles the message from the source.
     * @param source the source in the distributed system
     * @param message message
     */
    protected abstract void handleMessage(String source, String message);
    
    /**
     * Gets the parameters from the specified configuration file.
     * @param fileName the name of the specified configuration file
     * @return parameters
     */
    protected Map<String, String> getParameters(String fileName) {
        Map<String, String> parameters = new HashMap<String, String>();
        RandomAccessFile file = null;
        try {
            file = new RandomAccessFile(fileName, "r");
            String line = null;
            while ((line = file.readLine()) != null) {
                int index = line.indexOf('=');
                String key = line.substring(0, index).trim();
                String value = line.substring(index + 1).trim();
                parameters.put(key, value);
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
        return parameters;
    }
    
    /**
     * Prints the parameters.
     */
    protected void printParameters() {
        StringBuilder sb = new StringBuilder();
        sb.append("name = ").append(this.name).append('\n');
        sb.append("peers = ");
        if (!this.peers.isEmpty()) {
            for (String peer : this.peers.keySet()) {
                sb.append(peer).append(", ");
            }
            sb.setLength(sb.length() - 2);
        }
        printLog(sb.toString());
    }
    
    /**
     * Prints the log.
     * @param operation operation
     * @param host host
     * @param text text
     */
    protected synchronized void printLog(OPERATION operation, String host, String text) {
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
        logWriter.println(sb.toString());
    }
    
    /**
     * Prints the log.
     * @param text
     */
    protected synchronized void printLog(String text) {
        logWriter.println(text);
    }
    
    /**
     * Peer node in the distributed system.
     *
     */
    private class Peer {
        /**
         * Name of the peer.
         */
        private String name;
        /**
         * IP address of the peer.
         */
        private String address;
        /**
         * Backend port of the peer.
         */
        private int backendPort;
        /**
         * Socket which sends data to TCP server of the peer.
         */
        private Socket clientSocket;
        /**
         * Reader which reads data from the TCP server of the peer.
         */
        private BufferedReader clientReader;
        /**
         * Writer which writes data to the TCP server of the peer. 
         */
        private PrintWriter clientWriter;
        /**
         * Writer which writes data to the TCP client of the peer.
         */
        private PrintWriter serverWriter;
        
        /**
         * Constructs a peer node with specified name, address and backend port.
         * @param name name
         * @param address address
         * @param backendPort backend port
         */
        Peer(String name, String address, int backendPort) {
            this.name  = name;
            this.address = address;
            this.backendPort = backendPort;
        }
    }
    
    /**
     * Request sender.
     *
     */
    private class RequestSender implements Runnable {
        /**
         * Destination.
         */
        private String destination;
        /**
         * Request.
         */
        private String request;
        /**
         * A map with destinations as keys and responses as values.
         */
        private Map<String, String> responses;
        
        /**
         * Constructs a request sender.
         * @param destination destination
         * @param request request
         * @param responses a map with destinations as keys and responses as values
         */
        RequestSender(String destination, String request, Map<String, String> responses) {
            this.destination = destination;
            this.request = request;
            this.responses = responses;
        }
        
        /**
         * Sends a request and stores the response in the map.
         */
        @Override
        public void run() {
            String response = sendRequest(destination, request);
            synchronized(responses) {
                responses.put(destination, response);
            }
        }
    }
    
    /**
     * TCP server of this node.
     *
     */
    private class TCPServer implements Runnable {
        /**
         * Backend port of this node.
         */
        private int backendPort;
        
        /**
         * Constructs a TCP server with specified backend port.
         * @param backendPort
         */
        TCPServer(int backendPort) {
            this.backendPort = backendPort;
        }
        
        /**
         * Keeps listening. When a socket is accepted, a TCP client handler socket is launched.
         */
        @Override
        public void run() {
            printLog("Launch TCP server.");
            ServerSocket serverSocket = null;
            try {
                serverSocket = new ServerSocket(backendPort);
                while (true) {
                    Socket socket = serverSocket.accept();
                    new Thread(new TCPClientHandler(socket)).start();
                }
            } catch (IOException e) {
            } finally {
                try {
                    serverSocket.close();
                } catch (IOException e) {}
            }
        }
    }
    
    /**
     * TCP client handler.
     *
     */
    private class TCPClientHandler implements Runnable {
        /**
         * Socket of this connection.
         */
        private Socket socket;
        
        /**
         * Constructs a TCP client handler with specified socket.
         * @param socket
         */
        TCPClientHandler(Socket socket) {
            this.socket = socket;
        }
        
        /**
         * Receives requests from the client and handles the requests.
         */
        @Override
        public void run() {
            BufferedReader reader = null;
            PrintWriter writer = null;
            try {
                reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                writer = new PrintWriter(socket.getOutputStream(), true);
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
            } finally {
                try {
                    socket.close();
                    reader.close();
                } catch (IOException e) {}
                writer.close();
            }
        }
    }
    
    /**
     * UDP server of this node.
     *
     */
    private class UDPServer implements Runnable {
        /**
         * Receives and handles messages.
         */
        @Override
        public void run() {
            printLog("Launch UDP server.");
            try {
                byte[] buf = new byte[BUF_SIZE];
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                while (true) {
                    datagramSocket.receive(packet);
                    buf = packet.getData();
                    String message = new String(buf, 0, packet.getLength());
                    int index = message.indexOf(',');
                    String source = message.substring(0, index);
                    message = message.substring(index + 1);
                    printLog(OPERATION.RECEIVE_MESSAGE, source, message);
                    handleMessage(source, message);
                }
            } catch (IOException e) {
            } finally {
                datagramSocket.close();
            }
        }
    }
}

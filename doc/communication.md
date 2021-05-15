# Communication
## Connection Manager
[Connection manager](../src/ConnectionManager.java) is the base class of all other classes. It hides all network complexities and provides various methods for subclasses. In the distributed system, each node has a name, an IP address and a backend port. The connection manager reads the information from the [configuration file](../conf/connection_manager.conf). When sending text, subclasses only need the name of the target node.

Both TCP and UDP are utilized in the connection manager. Text sent from a TCP client to a TCP server is called a request, and that sent from a TCP server to a TCP client is called a response. After sending a request, the TCP client will block until it receives the response if the connection does not fail. Text sent from a UDP client to a UDP server is called a message. The UDP server does not respond, and the UDP client does not block.

## Sample Node
[Sample node](../src/SampleNode.java) extends connection manager. It provides a shell to test the connection manager.
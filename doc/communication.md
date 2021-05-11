# Communication
## Connection Manager
[Connection manager](../src/ConnectionManager.java) is the base class for all other classes. It hides all network complexities and provides various methods for subclasses to use. In the distributed system, each node has a name, an IP address and a backend port. The connection manager reads the information from the [configuration file](../conf/connection_manager.conf). When sending text, subclasses only need the name of the target node.

Both TCP and UDP are utilized in the connection manager. Text sent from TCP client to TCP server is called request, and that sent from TCP server to TCP client is called response. After sending a request, the TCP client will block until it receives the response if the connection does not fail. Text sent from UDP client to UDP server is called message. When receiving a message, the UDP server will not respond. Thus, the UDP client will not block after sending a message.

## Sample Node
[Sample node](../src/SampleNode.java) extends connection manager. It provides a shell to test the connection manager.
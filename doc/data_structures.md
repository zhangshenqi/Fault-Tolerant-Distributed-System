# Data Structures
                             Connection Manager
                             /                \
               Fault Detector                  User
                   /    \                     /    \
    Replica Manager      Replica   Active User      Passive User
                           / \
             Active Replica   Passive Replica

## Connection Manager
[Connection manager](../src/ConnectionManager.java) is the base class for all other classes. It hides all network complexities and provides various methods for subclasses to use.

In the distributed system, each node has a name, an IP address and a backend port. The connection manager reads the information from the [configuration file](../conf/connection_manager.conf).

## Fault Detector
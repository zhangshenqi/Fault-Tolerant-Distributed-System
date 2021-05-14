# Fault-Tolerant-Distributed-System
## Introduction
This project is about building fault-tolerant distributed systems based on replication. Typically, there are four kinds of nodes in a distributed system: replica manager, fault detector, replica and user. Replication is utilized to ensure the consistency and durability of replicas.
- [Communication](doc/communication.md)
- [Fault Detection](doc/fault_detection.md)
- [No Replication](doc/no_replication.md)
- [Active Replication](doc/active_replication.md)
- [Passive Replication](doc/passive_replication.md)

## Inheritance
```
                        Connection Manager
                         /                \
           Fault Detector                  User
               /    \                     /    \
Replica Manager      Replica   Active User      Passive User
                       / \
         Active Replica   Passive Replica
```

## Quick Start
```
      RM
       |
      GFD
    /  |  \
LFD1 LFD2 LFD3
  |    |    |
 RP1  RP2  RP3

USR1 USR2 USR3
````
A sample distributed system contains a replica manager, a global fault detector, three local replicas, three replicas and three users.

Compile [java programs](src):
```
java *.java
```
Prepare [configuration files](conf).

Launch the distributed system in no replication mode:
```
java ReplicaManager RM
java FaultDetector GFD
java FaultDetector LFD1
java FaultDetector LFD2
java FaultDetector LFD3
java Replica RP1
java Replica RP2
java Replica RP3
java User USR1
java User USR2
java User USR3
```
Launch the distributed system in active replication mode:
```
java ReplicaManager RM
java FaultDetector GFD
java FaultDetector LFD1
java FaultDetector LFD2
java FaultDetector LFD3
java ActiveReplica RP1
java ActiveReplica RP2
java ActiveReplica RP3
java ActiveUser USR1
java ActiveUser USR2
java ActiveUser USR3
```
Launch the distributed system in passive replication mode:
```
java ReplicaManager RM
java FaultDetector GFD
java FaultDetector LFD1
java FaultDetector LFD2
java FaultDetector LFD3
java PassiveReplica RP1
java PassiveReplica RP2
java PassiveReplica RP3
java PassiveUser USR1
java PassiveUser USR2
java PassiveUser USR3
```
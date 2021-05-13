# Fault Detection
## Fault Detector
[Fault detector](../src/FaultDetector.java) extends connection manager. It sends heartbeats to parents and receives heartbeats from children. If a heartbeat is received, the corresponding child is considered alive. If the number of consecutive missing heartbeats from a child reaches the tolerance, a child is considered dead. Upon detecting that a child becomes alive or dead, a fault detector will notify its parents. Also, if it is told by a child that a node becomes alive or dead, it will forward that notice to its parents. The fault detector reads its parents and children from the [configuration file](../conf/fault_detector.conf).

## Fault Detection System
Multiple fault detectors can form a multi-level fault detection system. Typically, if every fault detector has one parent except the top one, the system is in a tree structure. If a node in the system becomes alive or dead, the top fault detector will get to know.

If there are many leaves in the tree structure, increasing the level number can average the load in each fault detector. However, the transmission time from leaves to the root also increases. A good fault detection design should reach a balance.
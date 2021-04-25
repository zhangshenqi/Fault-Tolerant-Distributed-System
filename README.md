# Fault-Tolerant-Distributed-System
Fault-tolerant distributed systems based on replication.

## Introduction
This project is about using replication to build fault-tolerant distributed systems. Typically, there are four kinds of nodes in a distributed system: replica manager, fault detector, replica and user. The replica manager is the control center of the system. Fault detectors use heartbeats to detect each other. Replicas store data and handle requests from users. Users send requests to read or modify the data and get responses.

## Communication
Nodes utilize both TCP and UDP to communicate. Connection manager is the base class. It hides all network complexities.
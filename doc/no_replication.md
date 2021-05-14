# No Replication
## Replica Manager
[Replica manager](../src/ReplicaManager.java) extends fault detector. It is the control center of the distributed system. In the fault detection system, it is the root node. So it knows the states of all other fault detectors. It stores the current alive replicas, which is also know as membership. Members in the membership are in chronological order: Replicas which become alive earlier rank higher. The first member is the primary replcia. When the membership changes, the replica manager will send the membership to all currently alive replicas. Names of all replicas are in the [configuration file](../conf/replica_manager.conf).

There is a shell after launching the replica manager. You can set the the heartbeat interval and tolerance of the fault detection system. You can also set the checkpoint interval of replicas.

## Replica
[Replica](../src/Replica.java) extends fault detector. In the fault detection system, replicas are leaf nodes. Each replica stores a copy of the data. It receives requests from users, reads or modifies the data and sends responses back to users. It gets the initial values of the data from the [configuration file](../conf/replica.conf).

## User
[User](../src/User.java) extends connection manager. It sends user requests to replicas and shows responses. A timestamp is attached to each user request. This ensures that each user request is unique. The user reads the name of the replica manager from the [configuration file](../conf/user.conf). Before sending user requests, it asks the replica manager for the membership. Then it sends the same request to all replicas in the membership and get responses.

If shell environment variable ENABLE_AUTO_TEST is set, then the user will automatically send user requests to test the distributed system. Otherwise, there is a shell for manual testing.

## Distributed System in No Replication Mode
In no replication mode, there is no interaction among replicas. When there are multiple replicas and users, data in different replicas can be inconsistent, because each replica handles users concurrently and there is no total order. When a backup replica is launched, there is also no mechanism to get the latest data.
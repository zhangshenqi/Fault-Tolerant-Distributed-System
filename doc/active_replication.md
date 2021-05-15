# Active Replication
## Active Replica
[Active replica](../src/ActiveReplica.java) extends replica. It is the replica which performs active replication.

### Total Order
A voting mechanism is utilized to ensure that all replicas handle user requests in the same order. When receiving a user request, the replica will not handle it immediately. Instead, the request is stored in the user request set. The primary replica will initiate a vote to ask backups whether they also receive this request. Only when all backups answer yes, the primary will ask them to handle it. Otherwise, the primary will tell backups to give up, and nothing will be done.

### Logging and Checkpointing
Each replica maintains a log and a checkpoint. Whenever a user request is handled, it is appended to the log. Checkpoint is a snapshot of the data. The replica periodically empties the log and updates the checkpoint. Therefore, user requests in the log are all later than the checkpoint.

### Restoration
After a replica is launched, it gets to know whether it is primary or backup when receiving the first membership from the replica manager. If it is primary, which means that it is the first replica in the system, no restoration is needed. If the replica is a backup, then it needs to restore states.

Before the restoration, the new replica first asks all other replicas to be quiescent. This ensures that all replicas stop handling user requests. Then it asks other replicas in the membership one by one until a response is received. The response contains three parts: checkpoint, log and unhandled user requests in the set. After deserializing the checkpoint and re-handling requests in the log, the data is restored. Next, the replica will check those unhandled requests in the response. Those that are not in its own user request set will be stored in a separate restored user request set. Later, when handling those requests, the replica will not send responses to users. Finally, the replica asks other replicas to stop the quiescent stage.

### Upgrade
The primary replica in the system can die at any time. If it dies after telling some backups to handle a user request and before telling the remaining, backups may have inconsistent data.

After the death of the old primary, a backup replica will upgrade to be the new primary. During the upgrade, it synchronizes all replicas in case there is inconsistency. In the synchronization, the new primary checks if some replicas have handled a user request while others are waiting for the old primary's instruction on that request. If this is true, the new primary will tell those waiting replicas to handle the request.

## Active User
[Active User](../src/ActiveUser.java) extends user. It is the user in active replication mode. Similar to the user in no replication node, it gets membership from the replica manager and sends a request to all replicas. All responses should be the same, except those null responses due to connection failures. If there are different responses, the user will print the error and exit. This should never happen.

## Distributed System in Active Replication Mode
### Speed
For the sake of total order, a voting mechanism is implemented, which brings much more network overhead. User requests, even those read-only requests, are handled one by one. Concurrency is not utilized. Compared with passive replication, active replication is slower.

### Down Time
Theoretically, in this active replication model, as long as there is at least one replica, users will always get the response.

### Restoration
In active replication mode, a new backup replica will start the restoration immediately once it receives the membership. If it can receive the response for restoration from any of the other replicas, the restoration can succeed. The only corner case is that all other replicas die before the new replica asks for help. This should be very rare. Compared with that in passive replication, restoration in active replication is faster and safer.
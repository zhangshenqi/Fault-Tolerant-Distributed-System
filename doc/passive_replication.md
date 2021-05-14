# Passive Replication
## Passive Replica
[Passive replica](../src/PassiveReplica.java) extends replica. It is the replica which performs passive replication.

### Checkpointing
Timestamps of requests from one user are monotonically increasing. For each user, the replica stores the timestamp of the latest handled request after the last checkpoint. After receiving a user request, the primary replica will handle it immediately, update the latest timestamp of this user and respond. A backup replica will not handle the request. It simply sends "ACK" to the user. If the request is not read-only, the backup stores it.

Periodically, the primary serializes the data and stored timestamps to be the checkpoint and sends it to all backups. A backup deserializes the data and timestamps. Then it clears those stored user requests which are earlier than the timestamp for each user.

### Restoration
After a new backup replica is launched, the primary will get the updated membership. In the next checkpointing, the new backup can restore the data and timestamps.

### Upgrade
When a backup replica upgrades to be the new primary, it silently handles all stored user requests.

## Passive User
[Passive User](../src/PassiveUser.java) extends user. It is the user in passive replication mode. Before sending a user request, it gets membership from the replica manager. If this is the first user request or the primary replica is different from that in the previous membership, the user does not know if the primary replica is upgrading now. If this is true and the user sends the request, the primary will handle it silently. Therefore, to avoid this possible error, the user first asks the primary about the upgrade, and the primary replies when the upgrade is finished. Next, the user sends the request. If it gets the response, which means that the primary is alive, it sends the request to backups. This mechanism ensures that, only when the primary handles the request and responds, the backups will get that request. Without this mechanism, there is a possible corner case: The request is not read-only. The primary dies suddenly, and the user does not get the response. The user still sends the request to backups and backups store this request. Later, a backup upgrades to be the new primary. During the upgrade, it will silently handle this request, making the data incorrect.

## Distributed System in Passive Replication Mode
### Speed
There is no need to ensure total order in passive replication. Each replica can handle multiple concurrent users with readers-writer locks to protect shared data structures. The speed is much faster than that in active replication.

### Down Time
If the primary dies and the future primary has not receive the new membership, there is no primary replica in the system. During this time, users cannot get responses.

### Restoration
In passive replication mode, there is no separate mechanism for restoration. a new backup replica waits for the primary's checkpoint to restore. If all other replicas dies before the new backup receives the checkpoint, the new backup cannot restore. Compared with that in active replication, restoration in passive replication has more delay and risk.
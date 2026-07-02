# Kafka KRaft: When 1 Node Down Can Make Cluster Unstable

This README explains cases where a **3-node Kafka KRaft cluster** can become unstable when **only 1 node is down**.

This file focuses only on KRaft failure behavior, not the full setup guide.

---

## 1. Reference Architecture

3-node KRaft cluster:

```text
kafka-1 = broker + controller
kafka-2 = broker + controller
kafka-3 = broker + controller
```

Example IPs:

| Node | Private IP | Role |
|---|---:|---|
| kafka-1 | 10.116.0.2 | broker + controller |
| kafka-2 | 10.116.0.4 | broker + controller |
| kafka-3 | 10.116.0.3 | broker + controller |

KRaft uses:

| Port | Purpose |
|---:|---|
| 9092 | Kafka broker/client traffic |
| 9093 | KRaft controller quorum traffic |

Recommended HA topic config:

```text
replication-factor = 3
min.insync.replicas = 2
producer acks = all
```

Expected normal behavior:

```text
1 node down = OK
2 nodes down = not OK
```

But 1 node down can still cause instability in some cases.

---

## 2. Healthy 1-Node-Down State

If `kafka-3` is down, healthy topic state should look like:

```text
Partition 0 Leader: 1 Replicas: 3,1,2 Isr: 1,2
Partition 1 Leader: 1 Replicas: 1,2,3 Isr: 1,2
Partition 2 Leader: 2 Replicas: 2,3,1 Isr: 2,1
```

Meaning:

```text
broker 3 = down / not in-sync
broker 1 and broker 2 = alive and in-sync
```

Because `min.insync.replicas=2`, producer with `acks=all` should still work.

Test producer:

```bash
echo "order while kraft node 3 is down" | /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server 10.116.0.2:9092,10.116.0.4:9092 \
  --topic smartserve-orders \
  --command-property acks=all
```

Test consumer:

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server 10.116.0.2:9092,10.116.0.4:9092 \
  --topic smartserve-orders \
  --from-beginning \
  --group test-kraft-node3-down-$(date +%s) \
  --timeout-ms 10000
```

If the consumer exits with this after receiving messages:

```text
TimeoutException
Processed a total of X messages
```

that is normal when using `--timeout-ms`. It only means no new messages arrived before the timeout.

---

## 3. Case 1: ISR Was Already Unhealthy Before the Node Went Down

This is the most important case.

Healthy before failure:

```text
Isr: 1,2,3
```

If `kafka-3` goes down:

```text
Isr: 1,2
```

This is still OK.

Bad before failure:

```text
Isr: 1,2
```

Then if `kafka-2` goes down:

```text
Isr: 1
```

Now producer writes fail because the topic requires:

```text
min.insync.replicas = 2
acks = all
```

Expected producer errors:

```text
NOT_ENOUGH_REPLICAS
NOT_ENOUGH_REPLICAS_AFTER_APPEND
REQUEST_TIMED_OUT
```

Check ISR:

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 10.116.0.2:9092 \
  --describe \
  --topic smartserve-orders
```

Healthy before HA testing:

```text
Isr: 1,2,3
```

Unhealthy before HA testing:

```text
Isr: 1,2
```

Conclusion:

```text
1 node down is safe only if all replicas were healthy before the failure.
```

---

## 4. Case 2: Client Bootstraps Only to the Down Broker

Bad client config:

```properties
bootstrap.servers=10.116.0.3:9092
```

If `kafka-3` is down, the client cannot discover the cluster.

Expected errors:

```text
Bootstrap broker disconnected
Connection refused
Timed out waiting for a node assignment
```

Good client config:

```properties
bootstrap.servers=10.116.0.2:9092,10.116.0.4:9092,10.116.0.3:9092
```

During a known failure, use alive brokers:

```properties
bootstrap.servers=10.116.0.2:9092,10.116.0.4:9092
```

Conclusion:

```text
Clients should never depend on only one bootstrap broker.
```

---

## 5. Case 3: Failed Node Was the Partition Leader

Example before failure:

```text
Partition 0 Leader: 3
Isr: 1,2,3
```

Then `kafka-3` goes down.

Kafka must elect a new leader:

```text
Leader: 1 or 2
Isr: 1,2
```

Possible temporary impact:

```text
producer pause
consumer pause
metadata refresh
short timeout
leader election delay
```

This is normal and should recover if the remaining replicas are healthy.

Possible producer errors during the transition:

```text
NOT_LEADER_OR_FOLLOWER
LEADER_NOT_AVAILABLE
REQUEST_TIMED_OUT
NETWORK_EXCEPTION
```

Conclusion:

```text
Leader failure can cause short instability even when the cluster is healthy.
```

---

## 6. Case 4: Producer Timeout and Retry Config Is Too Strict

Strict test config:

```properties
request.timeout.ms=3000
delivery.timeout.ms=8000
retries=0
```

This is useful for seeing errors during testing, but bad for production.

During leader election or metadata refresh, 3-8 seconds may be too short.

Better production-style producer config:

```properties
acks=all
enable.idempotence=true
retries=2147483647
request.timeout.ms=30000
delivery.timeout.ms=120000
```

Conclusion:

```text
A recoverable Kafka failover can look like message failure if producer timeout/retry config is too strict.
```

---

## 7. Case 5: `min.insync.replicas` Is Too High

For a 3-broker cluster, recommended safe setting:

```text
replication-factor = 3
min.insync.replicas = 2
```

Bad setting:

```text
min.insync.replicas = 3
```

If one broker goes down:

```text
Only 2 ISR remain
Kafka requires 3 ISR
Writes fail
```

Expected errors:

```text
NOT_ENOUGH_REPLICAS
NOT_ENOUGH_REPLICAS_AFTER_APPEND
```

Conclusion:

```text
For 3-node Kafka that must tolerate 1 node failure, use min.insync.replicas=2.
```

---

## 8. Case 6: Topic Replication Factor Is Too Low

Bad topic:

```text
ReplicationFactor: 1
Replicas: 3
Isr: 3
```

If broker `3` goes down:

```text
No available replica
Partition unavailable
Producer fails
Consumer cannot read that partition
```

Good HA topic:

```text
ReplicationFactor: 3
Replicas: 1,2,3
Isr: 1,2,3
```

Conclusion:

```text
Replication factor 1 is not highly available.
```

---

## 9. Case 7: Remaining Brokers Are Overloaded

Before failure:

```text
kafka-1 + kafka-2 + kafka-3 share traffic
```

After one broker is down:

```text
kafka-1 + kafka-2 handle all traffic
```

If remaining brokers are overloaded:

```text
producer timeout
consumer lag
slow fetch
replication lag
ISR shrink
```

Common causes:

```text
CPU high
memory pressure
slow disk
network saturation
large GC pause
too many partitions
```

Useful checks:

```bash
top
free -h
df -h
```

Optional if installed:

```bash
iostat -xz 1
```

Conclusion:

```text
HA requires spare capacity on the surviving brokers.
```

---

## 10. Case 8: Remaining Controllers Cannot Communicate on Port 9093

KRaft controller quorum uses port `9093`.

With 3 controllers:

```text
need 2 alive
```

If `kafka-3` is down, then `kafka-1` and `kafka-2` must still communicate on `9093`.

If firewall or network blocks `9093` between the two alive nodes, KRaft quorum becomes unstable.

Check controller ports:

```bash
nc -vz kafka-1 9093
nc -vz kafka-2 9093
nc -vz kafka-3 9093
```

When `kafka-3` is down, `kafka-3` will fail, but `kafka-1` and `kafka-2` must succeed.

Check quorum:

```bash
/opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server 10.116.0.2:9092 \
  describe \
  --status
```

Healthy output should show a valid controller leader:

```text
LeaderId: 1 or 2
Voters: [1, 2, 3]
```

Conclusion:

```text
In KRaft, 9093 is critical for metadata quorum.
```

---

## 11. Case 9: Active KRaft Controller Goes Down

One node is the active KRaft controller leader.

If that node goes down, remaining controllers elect a new controller.

Possible temporary impact:

```text
metadata refresh delay
topic admin command delay
brief produce/consume pause
controller election delay
```

This should recover if the remaining 2 controllers are healthy.

Check controller status:

```bash
/opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server 10.116.0.2:9092 \
  describe \
  --status
```

Conclusion:

```text
Active controller failure can cause a short control-plane pause, but should recover with 2 controllers alive.
```

---

## 12. Case 10: Consumer Group Coordinator Was on the Failed Broker

Kafka consumer groups have a group coordinator.

If the coordinator was on the failed broker:

```text
consumer group needs new coordinator
rebalance happens
offset commit may pause
```

Possible errors:

```text
Coordinator not available
Rebalance in progress
CommitFailedException
```

This is usually temporary.

Conclusion:

```text
Consumer instability during one broker failure is often just consumer group rebalance.
```

---

## 13. Fast Diagnosis Commands

### Check topic ISR

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 10.116.0.2:9092 \
  --describe \
  --topic smartserve-orders
```

Good with one node down:

```text
Isr: 1,2
```

Bad:

```text
Isr: 1
```

### Check KRaft quorum

```bash
/opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server 10.116.0.2:9092 \
  describe \
  --status
```

You want a valid `LeaderId`.

### Check brokers

```bash
/opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server 10.116.0.2:9092,10.116.0.4:9092,10.116.0.3:9092
```

### Check logs

```bash
sudo journalctl -u kafka -n 100 --no-pager
```

Search important errors:

```bash
sudo journalctl -u kafka -n 300 --no-pager | \
grep -Ei "error|timeout|leader|isr|quorum|controller|not_enough|disconnect"
```

### Check ports

```bash
nc -vz kafka-1 9092
nc -vz kafka-2 9092
nc -vz kafka-3 9092

nc -vz kafka-1 9093
nc -vz kafka-2 9093
nc -vz kafka-3 9093
```

---

## 14. Summary Table

| Case | Impact when 1 node is down |
|---|---|
| ISR was already unhealthy | Writes may fail |
| Client uses only down broker | Client cannot connect |
| Failed node was leader | Short producer/consumer pause |
| Producer timeout too strict | Temporary failover becomes failed send |
| `min.insync.replicas=3` | Writes fail after one broker down |
| Replication factor = 1 | Partition unavailable |
| Remaining brokers overloaded | Timeout, lag, ISR shrink |
| 9093 blocked between remaining nodes | KRaft quorum unstable |
| Active controller down | Short control-plane pause |
| Consumer coordinator down | Consumer rebalance |

---

## 15. Recommended Stable Config

### Topic

```text
replication-factor = 3
min.insync.replicas = 2
```

### Producer

```properties
acks=all
enable.idempotence=true
retries=2147483647
request.timeout.ms=30000
delivery.timeout.ms=120000
```

### Client bootstrap

```properties
bootstrap.servers=10.116.0.2:9092,10.116.0.4:9092,10.116.0.3:9092
```

### KRaft controller quorum

```properties
controller.quorum.voters=1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093
```

### Listener config

```properties
listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
advertised.listeners=PLAINTEXT://<broker-private-ip>:9092
```

---

## 16. Main Lesson

A 3-node KRaft cluster can tolerate 1 node down only when:

```text
ISR still has 2 brokers
KRaft quorum still has 2 controllers
clients can reach alive brokers
producer retry/timeouts are reasonable
remaining brokers have enough capacity
topic replication factor is 3
min.insync.replicas is 2
```

The most important checks are:

```text
Topic ISR
KRaft quorum
Client bootstrap
Producer config
Remaining broker capacity
```

Healthy one-node-down state:

```text
Isr: 1,2
KRaft quorum leader exists
producer with acks=all works
consumer receives the message
```

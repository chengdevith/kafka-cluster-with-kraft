# Kafka KRaft 3-Node Setup

This README covers only the **Kafka KRaft setup**.  
KRaft means Kafka runs **without ZooKeeper**.

Target architecture:

```text
kafka-1 = broker + controller
kafka-2 = broker + controller
kafka-3 = broker + controller
```

---

## 1. Node Mapping

| Node | Private IP | KRaft node.id |
|---|---:|---:|
| kafka-1 | 10.116.0.2 | 1 |
| kafka-2 | 10.116.0.4 | 2 |
| kafka-3 | 10.116.0.3 | 3 |

On **all 3 nodes**, edit `/etc/hosts`:

```bash
sudo nano /etc/hosts
```

Add:

```text
10.116.0.2 kafka-1
10.116.0.4 kafka-2
10.116.0.3 kafka-3
```

Verify:

```bash
getent hosts kafka-1
getent hosts kafka-2
getent hosts kafka-3
```

---

## 2. Required Ports

KRaft uses:

| Port | Purpose |
|---:|---|
| 9092 | Kafka broker/client traffic |
| 9093 | KRaft controller quorum traffic |

Allow TCP `9092` and `9093` between the private IPs of all Kafka nodes.

You do **not** need ZooKeeper ports:

```text
2181
2888
3888
```

---

## 3. Install Java and Tools

Run on **all 3 nodes**:

```bash
sudo apt update
sudo apt install -y openjdk-17-jdk wget tar vim netcat-openbsd
```

Check:

```bash
java -version
```

---

## 4. Create Kafka User

Run on **all 3 nodes**:

```bash
sudo useradd -r -m -s /bin/bash kafka
```

If it already exists:

```bash
id kafka
```

---

## 5. Install Kafka 4.2.1

Run on **all 3 nodes**:

```bash
cd /opt

sudo wget https://downloads.apache.org/kafka/4.2.1/kafka_2.13-4.2.1.tgz

sudo tar -xzf kafka_2.13-4.2.1.tgz

sudo mv kafka_2.13-4.2.1 kafka

sudo chown -R kafka:kafka /opt/kafka
```

Check:

```bash
ls -ld /opt/kafka
/opt/kafka/bin/kafka-server-start.sh --version
```

Expected version:

```text
4.2.1
```

---

## 6. Create Data Directory

Run on **all 3 nodes**:

```bash
sudo mkdir -p /var/lib/kafka/data
sudo chown -R kafka:kafka /var/lib/kafka
```

---

## 7. Configure kafka-1

Run on **kafka-1**:

```bash
sudo mkdir -p /opt/kafka/config/kraft

sudo tee /opt/kafka/config/kraft/server.properties > /dev/null <<'EOF'
node.id=1
process.roles=broker,controller

listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
advertised.listeners=PLAINTEXT://10.116.0.2:9092

controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT

controller.quorum.voters=1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093

log.dirs=/var/lib/kafka/data

num.partitions=3
default.replication.factor=3
min.insync.replicas=2

offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2

auto.create.topics.enable=false
delete.topic.enable=true

log.retention.hours=168
EOF

sudo chown kafka:kafka /opt/kafka/config/kraft/server.properties
```

---

## 8. Configure kafka-2

Run on **kafka-2**:

```bash
sudo mkdir -p /opt/kafka/config/kraft

sudo tee /opt/kafka/config/kraft/server.properties > /dev/null <<'EOF'
node.id=2
process.roles=broker,controller

listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
advertised.listeners=PLAINTEXT://10.116.0.4:9092

controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT

controller.quorum.voters=1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093

log.dirs=/var/lib/kafka/data

num.partitions=3
default.replication.factor=3
min.insync.replicas=2

offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2

auto.create.topics.enable=false
delete.topic.enable=true

log.retention.hours=168
EOF

sudo chown kafka:kafka /opt/kafka/config/kraft/server.properties
```

---

## 9. Configure kafka-3

Run on **kafka-3**:

```bash
sudo mkdir -p /opt/kafka/config/kraft

sudo tee /opt/kafka/config/kraft/server.properties > /dev/null <<'EOF'
node.id=3
process.roles=broker,controller

listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
advertised.listeners=PLAINTEXT://10.116.0.3:9092

controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT

controller.quorum.voters=1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093

log.dirs=/var/lib/kafka/data

num.partitions=3
default.replication.factor=3
min.insync.replicas=2

offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2

auto.create.topics.enable=false
delete.topic.enable=true

log.retention.hours=168
EOF

sudo chown kafka:kafka /opt/kafka/config/kraft/server.properties
```

---

## 10. Generate KRaft Cluster ID

Run on **kafka-1 only**:

```bash
/opt/kafka/bin/kafka-storage.sh random-uuid
```

Example:

```text
q1Sh-9_ISia_zwGINzRvyQ
```

Use the **same cluster ID** on all 3 nodes:

```bash
export KRAFT_CLUSTER_ID="PASTE_YOUR_CLUSTER_ID_HERE"
```

Example:

```bash
export KRAFT_CLUSTER_ID="q1Sh-9_ISia_zwGINzRvyQ"
```

---

## 11. Format Kafka Storage

Run on **all 3 nodes**:

```bash
sudo -u kafka /opt/kafka/bin/kafka-storage.sh format   -t "$KRAFT_CLUSTER_ID"   -c /opt/kafka/config/kraft/server.properties
```

Expected:

```text
Formatting /var/lib/kafka/data
```

If you need to reset a lab node:

```bash
sudo systemctl stop kafka || true
sudo rm -rf /var/lib/kafka/data/*

sudo -u kafka /opt/kafka/bin/kafka-storage.sh format   -t "$KRAFT_CLUSTER_ID"   -c /opt/kafka/config/kraft/server.properties
```

Do not reformat production data unless you intend to delete it.

---

## 12. Create systemd Service

Run on **all 3 nodes**:

```bash
sudo tee /etc/systemd/system/kafka.service > /dev/null <<'EOF'
[Unit]
Description=Apache Kafka KRaft Broker and Controller
After=network.target

[Service]
Type=simple
User=kafka
Group=kafka
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=on-failure
RestartSec=5
LimitNOFILE=100000

[Install]
WantedBy=multi-user.target
EOF
```

Reload and enable:

```bash
sudo systemctl daemon-reload
sudo systemctl enable kafka
```

---

## 13. Start Kafka

Run on **all 3 nodes**:

```bash
sudo systemctl start kafka
sudo systemctl status kafka --no-pager
```

If failed:

```bash
sudo journalctl -u kafka -n 100 --no-pager
```

Follow logs:

```bash
sudo journalctl -u kafka -f
```

---

## 14. Check Ports

From `kafka-1`, check broker ports:

```bash
nc -vz kafka-1 9092
nc -vz kafka-2 9092
nc -vz kafka-3 9092
```

Check controller ports:

```bash
nc -vz kafka-1 9093
nc -vz kafka-2 9093
nc -vz kafka-3 9093
```

Expected:

```text
succeeded
```

---

## 15. Verify Brokers

Run from any node:

```bash
/opt/kafka/bin/kafka-broker-api-versions.sh   --bootstrap-server 10.116.0.2:9092,10.116.0.4:9092,10.116.0.3:9092
```

Expected:

```text
10.116.0.2:9092 (id: 1)
10.116.0.4:9092 (id: 2)
10.116.0.3:9092 (id: 3)
```

---

## 16. Verify KRaft Controller Quorum

Run:

```bash
/opt/kafka/bin/kafka-metadata-quorum.sh   --bootstrap-server 10.116.0.2:9092   describe   --status
```

Expected:

```text
LeaderId: 1 or 2 or 3
Voters: [1, 2, 3]
```

---

## 17. Create HA Topic

Run:

```bash
/opt/kafka/bin/kafka-topics.sh   --bootstrap-server 10.116.0.2:9092,10.116.0.4:9092,10.116.0.3:9092   --create   --topic smartserve-orders   --partitions 3   --replication-factor 3   --config min.insync.replicas=2
```

Describe:

```bash
/opt/kafka/bin/kafka-topics.sh   --bootstrap-server 10.116.0.2:9092   --describe   --topic smartserve-orders
```

Healthy expected state:

```text
PartitionCount: 3
ReplicationFactor: 3
Isr: 1,2,3
```

Order can be different.

---

## 18. Test Producer

Run:

```bash
/opt/kafka/bin/kafka-console-producer.sh   --bootstrap-server 10.116.0.2:9092,10.116.0.4:9092,10.116.0.3:9092   --topic smartserve-orders   --command-property acks=all
```

Type:

```text
order created with kraft mode
```

Press Enter.

Kafka 4.x prefers:

```text
--command-property
```

instead of:

```text
--producer-property
```

---

## 19. Test Consumer

Run in another terminal:

```bash
/opt/kafka/bin/kafka-console-consumer.sh   --bootstrap-server 10.116.0.2:9092,10.116.0.4:9092,10.116.0.3:9092   --topic smartserve-orders   --from-beginning   --group smartserve-kraft-test
```

Expected:

```text
order created with kraft mode
```

---

## 20. Test 1 Node Down

Stop Kafka on `kafka-3`:

```bash
sudo systemctl stop kafka
```

From `kafka-1`, describe topic:

```bash
/opt/kafka/bin/kafka-topics.sh   --bootstrap-server 10.116.0.2:9092   --describe   --topic smartserve-orders
```

Expected:

```text
broker 3 removed from ISR
Isr: 1,2
```

Produce while `kafka-3` is down:

```bash
echo "order while kraft node 3 is down" | /opt/kafka/bin/kafka-console-producer.sh   --bootstrap-server 10.116.0.2:9092,10.116.0.4:9092   --topic smartserve-orders   --command-property acks=all
```

Consume:

```bash
/opt/kafka/bin/kafka-console-consumer.sh   --bootstrap-server 10.116.0.2:9092,10.116.0.4:9092   --topic smartserve-orders   --from-beginning   --group test-kraft-node3-down-$(date +%s)   --timeout-ms 10000
```

Expected:

```text
order while kraft node 3 is down
```

If consumer exits with:

```text
TimeoutException
Processed a total of X messages
```

that is normal when using `--timeout-ms`.

---

## 21. Recover Node

On the down node, for example `kafka-3`:

```bash
sudo systemctl start kafka
```

Wait 10-30 seconds.

Check topic again:

```bash
/opt/kafka/bin/kafka-topics.sh   --bootstrap-server 10.116.0.2:9092   --describe   --topic smartserve-orders
```

Expected ISR recovery:

```text
Isr: 1,2,3
```

---

## 22. KRaft Quorum Rule

With 3 combined broker/controller nodes:

```text
3 controllers -> need 2 alive
```

So:

```text
1 node down = OK
2 nodes down = not OK
```

If 2 nodes are down:

```text
controller quorum lost
metadata operations fail
topic creation/deletion fails
leader election may fail
safe writes become unavailable
```

---

## 23. Useful Commands

Kafka status:

```bash
sudo systemctl status kafka --no-pager
```

Kafka logs:

```bash
sudo journalctl -u kafka -n 100 --no-pager
```

Follow Kafka logs:

```bash
sudo journalctl -u kafka -f
```

Broker check:

```bash
/opt/kafka/bin/kafka-broker-api-versions.sh   --bootstrap-server 10.116.0.2:9092,10.116.0.4:9092,10.116.0.3:9092
```

KRaft quorum check:

```bash
/opt/kafka/bin/kafka-metadata-quorum.sh   --bootstrap-server 10.116.0.2:9092   describe   --status
```

List topics:

```bash
/opt/kafka/bin/kafka-topics.sh   --bootstrap-server 10.116.0.2:9092   --list
```

Describe topic:

```bash
/opt/kafka/bin/kafka-topics.sh   --bootstrap-server 10.116.0.2:9092   --describe   --topic smartserve-orders
```

Delete topic:

```bash
/opt/kafka/bin/kafka-topics.sh   --bootstrap-server 10.116.0.2:9092   --delete   --topic smartserve-orders
```

---

## 24. Important Notes

Do not configure ZooKeeper in KRaft mode.

Do not add this:

```properties
zookeeper.connect=...
```

Do not run ZooKeeper service.

KRaft uses:

```properties
process.roles=broker,controller
controller.quorum.voters=1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093
```

Do not run this repeatedly on existing data unless resetting the node:

```bash
kafka-storage.sh format
```

Storage format is for initial setup or full reset.

---

## 25. Final Result

After setup:

```text
kafka-1 = broker + controller
kafka-2 = broker + controller
kafka-3 = broker + controller
```

No ZooKeeper.

Expected HA behavior:

```text
1 node down:
  produce/consume still works if ISR remains 2

2 nodes down:
  KRaft quorum lost
  safe writes unavailable
```

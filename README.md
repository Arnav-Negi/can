# Distributed Hash Table (DHT) Implementation

This project implements a Content-Addressable Network (CAN) based Distributed Hash Table (DHT) system. It provides a decentralized key-value storage mechanism that distributes data across multiple nodes in a peer-to-peer network.

## What is a CAN?

A Content-Addressable Network (CAN) is a distributed, decentralized P2P infrastructure that provides hash table functionality. CAN organizes nodes in a virtual n-dimensional coordinate space. This coordinate space is completely logical and bears no relation to any physical coordinate system.

Key features of CAN:

- The entire coordinate space is dynamically partitioned among all nodes in the system
- Each node owns a distinct zone within the overall space
- Each node maintains information about its neighboring zones
- Routing in CAN works by following a path through the coordinate space from source to destination
- When storing or retrieving data, the key is hashed to a point in the coordinate space, and the request is routed to the node responsible for that zone

CAN provides scalability, fault tolerance, and load balancing across all participating nodes in the network.

## Installation

```bash
go get github.com/Arnav-Negi/can
```

## Building

To build the application:

```bash
$ make clean
rm -rf ./cmd/cli/logs
rm -rf ./cmd/cli/certs
rm -rf ./testing/scale/logs
rm -rf ./testing/scale/certs
rm -rf ./testing/latency/logs
rm -rf ./testing/latency/certs
rm -rf ./testing/leaving/logs
rm -rf ./testing/leaving/certs
```

At the bootstrap node, root CA needs to be setup and generated from the script.

```bash
$ cd cmd/bootstrapper/certs/
$ chmod +x root-gen.sh
$ ./root-gen.sh

...GENERATES THE KEYS [DO NOT SHARE ca-key.pem]...
```

With this, you are ready to start using this service across your nodes.

## Running the DHT Node

### Starting a Bootstrap Node

To start the first node (bootstrap node):

```bash
go run <path/to/cmd/bootstrap/main.go> -dim <CAN_DIMENSIONS> -ip <HOST_IP> -port <HOST_PORT> -num_hashes <REPL_FACTOR>
```

This starts a bootstrap node that listens on the specified IP and port. Other nodes will connect to this bootstrap node to join the network.

### Joining an Existing Network

To start a node and have it join an existing network:

```bash
go run <path/to/cmd/cli/main.go> -bstrap-ip <BSTRAP_IP> -bstrap-port <BSTRAP_PORT> -ip <YOUR_IP>
```

This starts a node at `192.168.1.101:5001` and connects it to a bootstrap node at `192.168.1.100:5000`.

## Command Line Options

| Flag | Description | Default |
|------|-------------|---------|
| `-ip` | Node's IP address | 127.0.0.1 (localhost) |
| `-port` | Node's port (0 for random) | 0 |
| `-bstrap-ip` | Bootstrap node's IP address | 127.0.0.1 (localhost) |
| `-bstrap-port` | Bootstrap node's port | 5000 |

## Using the CLI

Once a node is running, you can interact with it using the following commands:

### Put a value in the DHT

```bash
put <key> <value>
```

Example:

```bash
put username alice
```

### Get a value from the DHT

```bash
get <key>
```

Example:

```bash
get username
```

### Delete a value from the DHT

```bash
delete <key>
```

Example:

```bash
delete username
```

### View node information

```bash
info
```

This shows the node's IP address and the coordinate zone it's responsible for.

### Get help

```bash
help
```

Displays available commands.

### Exit the application

```bash
exit
```

Properly leaves the DHT network and terminates the application.

## Examples

### Example 1: Start a bootstrap node

```bash
$ cd <root_dir>/cmd/boostrapper/
$ go run main.go -dim 7 -ip 172.20.10.13 -port 5000 -num_hashes 10

2025/04/17 20:43:51 Found IP: 172.20.10.13
2025/04/17 20:43:51 IP: 172.20.10.13, Port: 5000
2025/04/17 20:43:51 CAN dimensions: 7
.
.
.
```

### Example 2: Start a regular node and connect to the bootstrap

```bash
$ cd <root_dir>/cmd/cli/
$ go run main.go -bstrap-ip 172.20.10.13 -ip 172.17.56.88

Starting CAN DHT...
INFO[0000] Listening on IP 172.17.56.88:44833
.
.
.
```

### Example 3: Store and retrieve data

Once connected:

```bash
CAN-DHT > put favorite-color blue
put successful.

CAN-DHT > get favorite-color
blue
```

## Common Issues

- If a node can't connect to the bootstrap, verify that the bootstrap node is running and that the IP and port are correct.
- If you receive "Error getting value" messages, the key might not exist in the DHT or the network might be in an inconsistent state.
- For optimal performance, ensure nodes have stable network connections.

## Testing

The system includes test files to verify functionality and performance:

1. Scale test: Tests the DHT with multiple nodes
2. Latency test: Measures performance characteristics
3. Leaving test: Tests the graceful leaving of nodes

## Technical Details

The implementation uses a CAN-based DHT with the following characteristics:

- Each node owns a zone in a multi-dimensional coordinate space
- Keys are mapped to coordinates using a consistent hashing function
- Nodes route requests based on the coordinates of the target key
- Network operations are performed using gRPC for communication between nodes

## Achieved Project Objectives (as per Proposal)

### Caching to do Load Balancing

Our implementation includes caching frequently accessed key-value pairs to reduce query forwarding overhead. As described in the proposal under 3.2.5 Load Balancing, nodes maintain an LRU cache that helps in serving popular queries locally.

Whenever a <key,val> pair is accessed, on serving the access a side-effect is triggered to push the pair into the LRU cache. This helps short-circuit long forwards and to manage "hot-spots" as described in the paper. Entries have a TTL to prevent stale entries and to prevent purged entries from returning stale entries.

> Caching improves latency and reduces hop count for frequently accessed keys.

### Multi Hashes for Load Balancing and Replication

We implemented multiple hash functions to replicate each key-value pair across different zones. This helps with:

- Even load distribution (random hash selection during put)
- Lower latency (querying multiple replicas, race them)
- Fault tolerance (fallback to other replicas)

As outlined in 3.2.5 Load Balancing of the proposal.

> k hash functions → k nodes per key → increased availability and load distribution.

### Mutual TLS to Prevent Eavesdropping

We use mutual TLS (mTLS) to authenticate and encrypt all gRPC communication between nodes:

- Certificates are generated per node
- CSRs are signed by a root CA
- Bootstrapper provides public root CA key and distributes and signs them.

This satisfies 3.2.7 Data Encryption from the proposal.

> Ensures only trusted nodes can participate; all data-in-transit is encrypted.

### Full Support for get, put, and delete

The CLI supports all hash table operations with **history and arrow keys**:

```bash
Edit
put <key> <value>
get <key>
delete <key>
```

These operations work across the CAN and route to the appropriate node based on the hashed coordinates.

> Core DHT functionality implemented and exposed via an interactive CLI.

### Scale Testing

We performed tests using scripts under testing/scale/ to simulate large-scale networks. make clean also resets scale test data. The system was tested for:

- High query volume
- Rapid node joins/leaves
- Consistency and availability under scale

As per 2.3 Expected Deliverables and 3.3.2 Handling Large-Scale Failures.

> System maintains correctness and low hop-count under scale.

### Graceful Leaving

Nodes can gracefully leave the network by:

- Notifying neighbors
- Transferring key-value pairs
- Merging zones when possible
- Matches proposal section 3.2.4 Node leaving the CAN.

> Prevents zone orphaning and ensures continuous availability
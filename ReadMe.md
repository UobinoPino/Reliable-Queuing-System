# Reliable Queuing System

A distributed, fault-tolerant message queuing system implemented in Java, featuring leader election, state replication, and automatic failover capabilities. The system maintains consistency across multiple broker nodes using a consensus protocol similar to Raft.

## 📋 Table of Contents
- [Architecture](#architecture)
- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
- [Testing](#testing)
- [API Reference](#api-reference)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)

## 🏗️ Architecture

### System Overview
The Reliable Queuing System consists of two main components:

1. **Brokers**: Distributed nodes that manage message queues and coordinate through a leader-follower pattern
2. **Clients**: Applications that interact with brokers to read from and write to queues

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Client 1  │     │   Client 2  │     │   Client N  │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       └───────────────────┼───────────────────┘
                           │
       ┌───────────────────┼───────────────────┐
       │                   │                   │
┌──────▼──────┐     ┌──────▼──────┐     ┌──────▼──────┐
│  Broker 1   │◄────►  Broker 2   │◄────►  Broker N   │
│   (Leader)  │     │  (Follower) │     │  (Follower) │
└─────────────┘     └─────────────┘     └─────────────┘
```

### Core Components

#### Broker Components
- **SharedState**: Maintains replicated state including queues, client offsets, and operation logs
- **HeartbeatManager**: Monitors broker health and triggers leader election on failures
- **ElectionInfo**: Manages the leader election process and tracks election state
- **LogManager**: Handles log entry commits and executes queue operations
- **MessageDispatcher**: Routes and processes different message types
- **NetworkManager**: Manages connection pooling and efficient message transmission

#### Key Features
- **Leader Election**: Automatic leader election when the current leader fails
- **Log Replication**: All operations are replicated across brokers for consistency
- **Fault Tolerance**: System continues operating with broker failures
- **Persistent Storage**: Queue data and client offsets are persisted to disk
- **Connection Pooling**: Efficient socket management for high throughput

## ✨ Features

- **Distributed Consensus**: Leader-based replication with automatic failover
- **Multiple Queue Support**: Create and manage multiple named queues
- **Client State Tracking**: Persistent client IDs and read offsets
- **At-Least-Once Delivery**: Guaranteed message delivery with acknowledgments
- **Crash Recovery**: Automatic recovery from broker failures
- **Load Balancing**: Clients can connect to any broker
- **Persistent Storage**: Data survives broker restarts
- **High Performance**: Connection pooling and batch processing optimizations

## 📦 Requirements

- Java 21 or higher
- Maven 3.6 or higher
- Network connectivity between broker nodes
- Minimum 1GB RAM per broker node

## 🚀 Installation

### Building from Source

1. Clone the repository:
```bash
git clone https://github.com/UobinoPino/Reliable-queuing-system.git
cd reliable-queuing-system
```

2. Build the project using Maven:
```bash
mvn clean package
```

This will create the following JAR files in the `target` directory:
- `broker.jar` - Broker application
- `client.jar` - Client application
- `client-load-sim.jar` - Single client load simulator
- `multi-client-load-sim.jar` - Multi-client load simulator
- `multi_client_load_test.jar` - Multi-client load testing tool

## 💻 Usage

### Starting the First Broker

The first broker in a cluster must be started with the `--first` flag:

```bash
java -jar target/broker.jar --first
```

You'll be prompted to enter the port number (1024-65535) for the broker to listen on.

### Adding Additional Brokers

To add brokers to an existing cluster:

```bash
java -jar target/broker.jar
```

You'll be prompted for:
1. The port for this broker to listen on
2. The address of an existing broker in the cluster (format: `ip:port`)

### Running a Client

Start a client application:

```bash
java -jar target/client.jar
```

You'll be prompted for:
1. The port for the client to listen on
2. The address of a known broker (format: `ip:port`)

#### Client Operations

Once connected, clients can perform the following operations:
- **r**: Read new values from a queue
- **w**: Write a value to a queue
- **q**: Quit the application

### Example Session

1. Start the first broker:
```
$ java -jar target/broker.jar --first
======== RELIABLE QUEUING SYSTEM: BROKER ========
Enter the port the client should listen to: 5001
[INFO]: First broker 0 on Address[ip=192.168.1.100, port=5001]
```

2. Add a second broker:
```
$ java -jar target/broker.jar
======== RELIABLE QUEUING SYSTEM: BROKER ========
Enter the port the client should listen to: 5002
Enter the address of a known broker (<ip>:<port>): 192.168.1.100:5001
[INFO]: Joined cluster as broker 1
```

3. Start a client:
```
$ java -jar target/client.jar
======== RELIABLE QUEUING SYSTEM: CLIENT ========
Enter the port the client should listen to: 6001
Enter the address of a known broker (<ip>:<port>): 192.168.1.100:5001
[INFO]: Obtained new client ID: 0

What do you want to do?
    r) Read the new values from a queue
    w) Write a new value in a queue
    q) Quit the application
```

## 🧪 Testing

### Load Testing Tools

#### Single Client Load Simulation

Test with a single client performing multiple operations:

```bash
java -jar target/client-load-sim.jar
```

Configuration options:
- Broker address
- Total operations to perform
- Read/write ratio (0.0 to 1.0)
- Queue names to use

#### Multi-Client Load Simulation

Simulate multiple concurrent clients:

```bash
java -jar target/multi-client-load-sim.jar
```

Configuration options:
- Number of clients to simulate
- Operations per client
- Read/write ratio
- Queue configuration

#### Multi-Client Load Test

Advanced load testing with detailed metrics:

```bash
java -jar target/multi_client_load_test.jar
```

Features:
- Configurable client count
- Operations per client
- Detailed latency measurements
- Throughput analysis
- Failure tracking

### Performance Metrics

The load testing tools provide:
- **Throughput**: Operations per second
- **Latency**: Average operation latency
- **Success Rate**: Percentage of successful operations
- **Failure Analysis**: Breakdown of failed operations

## 📚 API Reference

### Message Types

#### Client Messages
- `ClientIdRequest`: Request a unique client ID
- `ReadRequest`: Read new values from a queue
- `WriteRequest`: Write a value to a queue

#### Broker Messages
- `BrokerJoinRequest`: Join the broker cluster
- `EntryPropagation`: Replicate log entries
- `Heartbeat`: Leader health check
- `NewLeaderNomination`: Propose new leader during election

### Queue Operations

#### Write Operation
```java
WriteRequest(
    String queueName,
    int value,
    int clientId,
    int operationId,
    Address clientAddress,
    Address contactedBrokerAddress
)
```

#### Read Operation
```java
ReadRequest(
    String queueName,
    int clientId,
    int operationId,
    Address clientAddress,
    Address contactedBrokerAddress
)
```

## ⚙️ Configuration

### Broker Configuration

Key parameters in `HeartbeatManager.java`:
- `HEARTBEAT_INTERVAL_MS`: 5000ms (heartbeat frequency)
- `HEARTBEAT_TIMEOUT_MS`: 10000ms (timeout before considering broker failed)
- `MISSABLE_HEARTBEATS`: 2 (missed heartbeats before removal)

### Network Configuration

In `NetworkManager.java`:
- `SOCKET_TIMEOUT`: 5000ms (socket connection timeout)
- `PER_PEER_POOL_SIZE`: 5000 (max pooled connections per peer)

### Persistence

Data is persisted in the `broker_data/` directory:
- `queues.json`: Queue contents
- `client_offsets.json`: Client read positions
- `client_addresses.json`: Client ID mappings

## 🔧 Troubleshooting

### Common Issues

#### Broker Won't Start
- **Port already in use**: Choose a different port
- **Cannot connect to cluster**: Verify the known broker address is correct

#### Client Connection Issues
- **Timeout waiting for response**: Check network connectivity to brokers
- **Client ID assignment failed**: Ensure at least one broker is running

#### Performance Issues
- **High latency**: Check network latency between nodes
- **Low throughput**: Increase connection pool size or add more brokers

### Logging

The system provides detailed logging with levels:
- `[INFO]`: Normal operations
- `[ERROR]`: Recoverable errors
- `[FATAL ERROR]`: System failures requiring restart

### Recovery Procedures

#### Leader Failure
The system automatically:
1. Detects leader failure via heartbeat timeout
2. Initiates leader election
3. Elects new leader based on log completeness
4. Resumes normal operations

#### Broker Crash Recovery
1. Restart the crashed broker
2. It will rejoin the cluster automatically
3. Receive state updates from the leader

## 🎯 Design Principles

### Consistency Model
- **Strong Consistency**: All brokers maintain identical state
- **Log-Based Replication**: Operations are ordered and replicated via a shared log
- **Majority Consensus**: Operations commit after acknowledgment from majority of brokers

### Fault Tolerance
- **Leader Election**: Automatic leader selection based on log completeness
- **Heartbeat Monitoring**: Continuous health checks with configurable timeouts
- **Graceful Degradation**: System continues with reduced broker count

### Performance Optimizations
- **Connection Pooling**: Reuses TCP connections for efficiency
- **Batch Processing**: Groups operations to reduce overhead
- **Asynchronous I/O**: Non-blocking network operations

## 📊 Project Structure

```
reliable-queuing-system/
├── src/main/java/it/polimi/ds/reliable_queuing_system/
│   ├── broker/
│   │   ├── Broker.java              # Main broker entry point
│   │   ├── SharedState.java         # Replicated state management
│   │   ├── HeartbeatManager.java    # Health monitoring
│   │   ├── ElectionInfo.java        # Election coordination
│   │   ├── LogManager.java          # Log operations
│   │   ├── MessageDispatcher.java   # Message routing
│   │   └── NetworkManager.java      # Connection management
│   ├── client/
│   │   ├── Client.java              # Client application
│   │   └── ClientState.java         # Client state machine
│   ├── messages/                    # Message protocol definitions
│   ├── utils/                       # Utility classes
│   └── test/                        # Load testing tools
├── pom.xml                          # Maven configuration
└── README.md                        # This file
```

## 🚦 Getting Started Quick Guide

1. **Build the project**:
   ```bash
   mvn clean package
   ```

2. **Start a 3-broker cluster**:
   ```bash
   # Terminal 1 - First broker
   java -jar target/broker.jar --first
   # Enter port: 5001
   
   # Terminal 2 - Second broker
   java -jar target/broker.jar
   # Enter port: 5002
   # Enter known broker: localhost:5001
   
   # Terminal 3 - Third broker
   java -jar target/broker.jar
   # Enter port: 5003
   # Enter known broker: localhost:5001
   ```

3. **Run a client**:
   ```bash
   # Terminal 4 - Client
   java -jar target/client.jar
   # Enter port: 6001
   # Enter broker: localhost:5001
   ```

4. **Test fault tolerance**:
    - Kill the leader broker (Terminal 1)
    - Observe automatic leader election
    - Client operations continue without interruption

## 📄 License

This project is part of the Distributed Systems course at Politecnico di Milano.

## 👥 Contributors

Leonardo Mantovani e Roberto Manea

## 📮 Contact

For questions and support, please open an issue in the project repository.
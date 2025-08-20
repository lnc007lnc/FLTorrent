# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FederatedScope is a comprehensive federated learning platform providing convenient usage and flexible customization for various FL tasks. It supports multiple federated learning paradigms including:

- Horizontal Federated Learning (HFL)
- Vertical Federated Learning (VFL) 
- Graph Federated Learning (GFL)
- Personalized Federated Learning
- Federated Hyperparameter Optimization
- Privacy attacks and defenses

## Development Commands

### Installation
```bash
# Clone and install
git clone https://github.com/alibaba/FederatedScope.git
cd FederatedScope

# Install basic version
pip install -e .

# Install with development tools
pip install -e .[dev]
pre-commit install

# Install application version (includes graph, nlp, speech dependencies)
bash environment/extra_dependencies_torch1.10-application.sh
```

### Testing
```bash
# Run specific test
python tests/run.py --test_dir tests --pattern test_toy_lr.py

# Run all tests
python tests/run.py

# List available tests
python tests/run.py --list_tests
```

### Running FL Experiments
```bash
# Basic FL run with config file
python federatedscope/main.py --cfg path/to/config.yaml

# Override config parameters
python federatedscope/main.py --cfg config.yaml federate.total_round_num 50 dataloader.batch_size 128

# Example runs
python federatedscope/main.py --cfg scripts/example_configs/femnist.yaml
python federatedscope/main.py --cfg federatedscope/cv/baseline/fedavg_convnet2_on_femnist.yaml
```

### Documentation Generation
```bash
cd doc
pip install -r requirements.txt
make html
```

## Architecture Overview

### Core Components (`federatedscope/core/`)

- **fed_runner.py**: Main FL course runner that orchestrates the entire federated learning process
- **workers/**: Contains Server and Client classes that define participant behaviors
- **trainers/**: Local training implementations for different algorithms (FedAvg, FedProx, Ditto, etc.)
- **aggregators/**: Federated aggregation strategies (FedAvg, FedOpt, robust aggregators)
- **configs/**: Configuration system using YACS for flexible hyperparameter management
- **data/**: Data handling, including translators and dataset wrappers
- **auxiliaries/**: Builder modules for models, optimizers, data loaders, etc.

### Application Domains
- **cv/**: Computer vision federated learning (ConvNet, ResNet on FEMNIST, CIFAR-10, CelebA)
- **nlp/**: Natural language processing (BERT, LSTM on Shakespeare, Reddit, Twitter)
- **gfl/**: Graph federated learning (GCN, GraphSAGE, GIN on citation networks)
- **mf/**: Matrix factorization for recommendation systems
- **vertical_fl/**: Vertical federated learning implementations
- **autotune/**: Federated hyperparameter optimization

### Key Design Patterns

1. **Event-Driven Architecture**: Uses message passing between server and clients
2. **Builder Pattern**: Extensive use of builder classes in `auxiliaries/` for component creation
3. **Configuration-Driven**: All experiments controlled via YAML configuration files
4. **Plugin System**: Easy registration of custom models, trainers, datasets via `register.py`

## Configuration System

The configuration system is built on YACS with a two-level tree structure:

- Configurations are defined in `federatedscope/core/configs/`
- Main config entry point: `config.py` with `global_cfg`
- Specific config modules: `cfg_data.py`, `cfg_model.py`, `cfg_training.py`, etc.
- Override configs via command line or YAML files

## Data Flow

1. **Data Loading**: `data_builder.py` creates datasets based on config
2. **Model Creation**: `model_builder.py` instantiates models per config
3. **Worker Setup**: Server and clients created via `worker_builder.py`
4. **Training Loop**: `fed_runner.py` orchestrates communication rounds
5. **Aggregation**: Server aggregates client updates via selected aggregator
6. **Evaluation**: Periodic evaluation on test sets

## Adding Custom Components

### Custom Models
```python
# 1. Create model in appropriate domain (cv/, nlp/, etc.)
# 2. Register in the domain's model_builder.py
# 3. Add config entries if needed
```

### Custom Trainers
```python
# 1. Inherit from base_trainer.py or torch_trainer.py
# 2. Implement train(), evaluate() methods
# 3. Register in trainer_builder.py
```

### Custom Datasets
```python
# 1. Create dataset class following existing patterns
# 2. Add to data_builder.py
# 3. Add splitter if needed in splitters/
```

## Running Modes

### Standalone Mode
Simulates multiple participants on single device:
```bash
python federatedscope/main.py --cfg config.yaml federate.mode standalone
```

### Distributed Mode  
Runs actual distributed FL across multiple devices:
```bash
# Server
python federatedscope/main.py --cfg server_config.yaml

# Clients (on different machines)
python federatedscope/main.py --cfg client_config.yaml
```

## Server-Client Connection Flow

### Client Join-In Process
FederatedScope uses a **synchronous startup mechanism** where the server waits for all expected clients to connect before starting FL training.

#### Key Components:
- **Server waiting loop**: `while self.join_in_client_num < self.client_num:` (server.py:265)
- **Client join process**: Each client sends `join_in` message with address info
- **Connection counting**: Server tracks `join_in_client_num` vs expected `client_num`

#### Startup Sequence:
1. **Server starts** and enters blocking wait loop
2. **Each client starts** and calls `join_in()` method
3. **Server receives** `join_in` messages and increments counter
4. **Server assigns** client IDs and records client info
5. **Training begins** only when `join_in_client_num == client_num`

#### Critical Configuration:
- `federate.client_num`: Expected number of clients (must match actual clients)
- `distribute.server_host/port`: Server connection endpoint
- `distribute.client_host/port`: Client connection details

#### Connection Message Monitoring:
Added custom connection monitoring system:
- **ConnectionMonitor** (client-side): Detects connection events (connect, disconnect, accept, reject)
- **ConnectionHandlerMixin** (server-side): Processes connection messages and saves to local files
- **File logging**: Connection events saved to `connection_logs/connection_messages.jsonl`

#### Important Notes:
- **Blocking behavior**: If any expected client fails to connect, server waits indefinitely
- **Order dependency**: Server must start before clients
- **Network requirements**: All participants must be able to reach server address
- **Synchronous design**: Classic federated learning startup pattern ensuring all parties ready

### Distributed FL Setup Script
Use `setup_distributed_fl.py` to automatically create 3-client + 1-server distributed setup:
```bash
python setup_distributed_fl.py
# Creates launch scripts, configs, and synthetic data for testing
```

## Network Topology Construction

### Custom Client-to-Client Topologies
FederatedScope now supports custom network topologies between clients, allowing direct peer-to-peer connections beyond the traditional server-client model. **This feature has been fully implemented and tested in real multi-process distributed environments.**

#### Key Features:
- **Multiple topology types**: star, ring, mesh, tree, custom
- **Automated construction**: Server orchestrates topology building after all clients join
- **Connection monitoring**: Real-time tracking of topology establishment
- **Comprehensive logging**: Connection events saved to `connection_logs/connection_messages.jsonl`
- **Production ready**: Successfully tested with real multi-process distributed FL

#### Core Components:
- **TopologyManager** (`federatedscope/core/topology_manager.py`): 
  - Computes topology structures for different types
  - Tracks connection establishment progress
  - Validates topology completion
  - Methods: `compute_topology()`, `record_connection_established()`, `is_topology_complete()`

- **Server topology integration** (`federatedscope/core/workers/server.py`):
  - `_construct_network_topology()`: Main orchestration method
  - Topology construction phase between client joining and training
  - Waits for topology completion before starting FL training
  - Line 415: Topology construction phase trigger

- **Client topology handling** (`federatedscope/core/workers/client.py`):
  - `callback_funcs_for_topology_instruction()`: Processes connection instructions
  - Establishes peer-to-peer connections according to topology
  - Reports connection status back to server
  - Line 659: Topology instruction processing

- **Connection monitoring enhancement** (`federatedscope/core/workers/connection_handler_mixin.py`):
  - Extended to track topology-specific connections
  - `callback_funcs_for_connection()`: Handles connection messages from clients
  - Real-time connection event logging

- **Configuration system** (`federatedscope/core/configs/cfg_topology.py`):
  - YACS integration for topology settings
  - Validation and type checking for topology configurations
  - Support for all topology types and custom graphs

#### Configuration:
Add to your YAML config:
```yaml
topology:
  use: True
  type: 'star'  # Options: star, ring, mesh, tree, custom
  timeout: 60.0
  max_connection_attempts: 3
  connection_retry_delay: 1.0
  require_full_topology: True
  save_construction_log: True
  log_dir: 'topology_logs'
  verbose: True
  
  # For custom topologies:
  custom_graph:
    1: [2, 3]    # Client 1 connects to clients 2 and 3
    2: [1, 4]    # Client 2 connects to clients 1 and 4
    3: [1]       # Client 3 connects to client 1
    4: [2]       # Client 4 connects to client 2
```

#### Execution Flow:
1. **All clients join**: Standard join_in phase completes
2. **Topology construction**: New phase between client joining and training
   - Server computes target topology based on configuration
   - Server sends `topology_instruction` messages to each client  
   - Clients establish connections according to instructions
   - Server monitors connection progress via `connect_msg` messages
   - **Server exits waiting loop** when topology is complete (or timeout reached)
3. **Training phase**: Standard FL training with established topology

#### Real-World Testing:
Successfully tested with real multi-process distributed FL using shell script automation:
```bash
# Run real distributed topology test (1 server + 3 clients)
./run_real_topology_test.sh

# Test Results (after bug fixes):
# ✅ Topology construction completed in ~14 seconds
# ✅ All expected peer connections established  
# ✅ Server successfully exited waiting loop after topology completion
# ✅ FL training proceeded normally with 2 training rounds
# ✅ Connection events properly logged to topology_logs/connection_messages.jsonl
# ✅ Star topology: Client1-Client2-Client3 connections verified
```

#### Key Implementation Fixes:
1. **Protobuf compatibility**: Updated protobuf files for modern versions using `python -m grpc_tools.protoc`
   - Fixed import statements in generated protobuf files
   - Resolved `TypeError: list indices must be integers or slices, not str`
2. **Boolean serialization**: Added support for boolean values in message serialization (message.py:84)
   - Added `elif type(value) == bool:` case to convert booleans to strings for protobuf
   - Fixed "The data type <class 'bool'> has not been supported" error
3. **Client ID propagation**: Fixed ConnectionMonitor to use correct client IDs after server assignment
   - Updated `ConnectionMonitor.client_id` after server assigns real client ID (client.py:659)
   - Fixed issue where ConnectionMonitor was using client_id = -1 instead of actual assigned IDs
4. **Message flow**: Proper handling of topology instruction and connection messages
   - Removed debug print statements, replaced with logger.debug() calls
   - Clean separation of connection monitoring from general logging

#### Testing:
```bash
# Comprehensive test suite (98% code coverage achieved)
python test_topology_construction.py        # Unit tests for TopologyManager and server logic
python test_end_to_end_topology.py         # Integration tests with mock distributed environment
python simple_connection_test.py           # Connection monitoring component tests

# Real multi-process testing (production validation)
./run_real_topology_test.sh               # 1 server + 3 clients with actual processes

# Example topology-enabled FL run
python federatedscope/main.py --cfg real_test_server.yaml    # Server config
python federatedscope/main.py --cfg real_test_client_1.yaml  # Client configs

# Test Results Summary:
# ✅ Unit tests: All topology types (star, ring, mesh, tree) working correctly
# ✅ Integration tests: Server-client topology orchestration verified
# ✅ Real distributed test: Multi-process FL with topology construction successful
# ✅ Connection monitoring: Real-time event tracking and logging functional
# ✅ Error handling: Timeout, retry, and failure scenarios tested
```

#### Documentation:
- **TOPOLOGY_USAGE_GUIDE.md**: Detailed usage instructions and examples
- **TESTING_RESULTS.md**: Comprehensive test results and validation
- **VALIDATION_SUMMARY.md**: System validation and performance metrics
- **example_topology_config.yaml**: Working configuration examples

## Common Debugging Tips

1. **Config Issues**: Check `cfg.freeze()` status and config merging order
2. **Data Issues**: Verify data path, splitting parameters, and dataset format
3. **Model Issues**: Ensure model architecture matches data dimensions
4. **Memory Issues**: Reduce batch size or model size in config
5. **Communication Issues**: Check server/client host/port settings for distributed mode
6. **Connection Issues**: 
   - Ensure `federate.client_num` matches actual number of clients
   - Check connection logs in `connection_logs/connection_messages.jsonl`
   - Verify all clients can reach server address
   - Start server first, then all clients
7. **Topology Construction Issues**:
   - Check topology configuration: `topology.use: True` and valid `topology.type`
   - Monitor topology logs in `topology_logs/connection_messages.jsonl`
   - Verify protobuf compatibility if seeing serialization errors
   - Ensure client ID assignment completed before topology construction
   - Check timeout settings: increase `topology.timeout` for large networks
   - Review server logs for "Starting network topology construction" message
   - Verify clients receive topology instructions: look for "🌐 Client X: Received topology instruction"

## Synchronous Training Mode - Detailed Workflow Analysis

### Core Training Flow After Local Training Completion

FederatedScope implements **synchronous federated learning** with strict message-based coordination between server and clients. The system uses a **passive waiting mechanism** where the server blocks until receiving sufficient client updates before proceeding to aggregation.

#### 1. Client-Side Training Completion Flow

**Key Function Chain:**
```
client.callback_funcs_for_model_para() → trainer.train() → _run_routine() → comm_manager.send()
```

**Training Result Calculation:**
- **File**: `federatedscope/core/trainers/trainer.py:233`
- **Sample Size Calculation**: Accumulated during training via `ctx.num_samples += ctx.batch_size` (torch_trainer.py:407)
- **Return Values**: `(num_samples, model_parameters, eval_metrics)`

**Message Sending Process** (`client.py:476`):
```python
self.comm_manager.send(
    Message(msg_type='model_para',
            sender=self.ID,
            receiver=[server_id],
            state=self.state,
            timestamp=timestamp,
            content=(sample_size, shared_model_para)))
```

#### 2. Server-Side Synchronous Coordination

**Message Routing** (`base_server.py:46`):
```python
self.register_handlers('model_para', self.callback_funcs_model_para)
```

**Synchronous Buffer Management** (`server.py:1075`):
```python
def callback_funcs_model_para(self, message: Message):
    # Extract message content
    content = message.content  # (sample_size, model_parameters)
    
    # Buffer management strategy
    if round == self.state:
        # Current round: save to main buffer
        self.msg_buffer['train'][round][sender] = content
    elif round >= self.state - self.staleness_toleration:
        # Stale but tolerable: save to stale buffer
        self.staled_msg_buffer.append((round, sender, content))
    else:
        # Too old: drop message
        self.dropout_num += 1
    
    # Critical: check if enough messages received for aggregation
    move_on_flag = self.check_and_move_on()
```

#### 3. Synchronization Check Mechanism

**Core Synchronization Logic** (`server.py:335`):
```python
def check_and_move_on(self):
    # Determine minimum required messages
    if self._cfg.asyn.use:
        min_received_num = self._cfg.asyn.min_received_num  # Async mode
    else:
        min_received_num = self._cfg.federate.sample_client_num  # Sync mode
    
    # Check if enough messages received
    if self.check_buffer(self.state, min_received_num):
        # Trigger aggregation
        aggregated_num = self._perform_federated_aggregation()
        self.state += 1
        
        # Start new training round
        if self.state < self.total_round_num:
            self._start_new_training_round(aggregated_num)
```

**Buffer Check Implementation** (`server.py:875`):
```python
def check_buffer(self, cur_round, min_received_num):
    cur_buffer = self.msg_buffer['train'][cur_round]
    # Synchronous condition: current messages + stale messages >= threshold
    return len(cur_buffer) + len(self.staled_msg_buffer) >= min_received_num
```

#### 4. Aggregation Weight Calculation

**FedAvg Weight Computation** (`clients_avg_aggregator.py:60`):
```python
def _para_weighted_avg(self, models):
    # Calculate total training dataset size
    training_set_size = sum(sample_size for sample_size, _ in models)
    
    # Weighted averaging per layer
    for i, (local_sample_size, local_model) in enumerate(models):
        if self.cfg.federate.ignore_weight:
            weight = 1.0 / len(models)  # Equal weight
        else:
            weight = local_sample_size / training_set_size  # Data-proportional weight
        
        # Apply weighted averaging
        if i == 0:
            avg_model[key] = local_model[key] * weight
        else:
            avg_model[key] += local_model[key] * weight
```

**Aggregation Execution** (`server.py:583`):
```python
def _perform_federated_aggregation(self):
    # Collect messages from current round
    for client_id in train_msg_buffer.keys():
        msg_list.append(train_msg_buffer[client_id])
        staleness.append((client_id, 0))
    
    # Include stale but usable messages
    for staled_message in self.staled_msg_buffer:
        staleness.append((client_id, self.state - state))
    
    # Execute aggregation
    result = aggregator.aggregate({
        'client_feedback': msg_list,
        'staleness': staleness,
    })
    
    # Update global model
    merged_param = merge_param_dict(model.state_dict().copy(), result)
    model.load_state_dict(merged_param, strict=False)
```

#### 5. Next Round Initialization

**Broadcasting New Global Model** (`server.py:638, 782`):
```python
def _start_new_training_round(self, aggregated_num=0):
    # Synchronous mode: broadcast to all participating clients
    self.broadcast_model_para(msg_type='model_para',
                              sample_client_num=self.sample_client_num)

def broadcast_model_para(self):
    model_para = self.models[0].state_dict()
    self.comm_manager.send(
        Message(msg_type='model_para',
                sender=self.ID,
                receiver=receiver,
                state=self.state,
                content=model_para))
```

#### 6. Key Synchronization Points

**Blocking Mechanism:**
- ✅ **Passive Waiting**: Server waits for messages, doesn't actively poll
- ✅ **Message-Driven**: Each `model_para` message triggers buffer check
- ✅ **Threshold Control**: `min_received_num = cfg.federate.sample_client_num`
- ✅ **State Synchronization**: `self.state` maintains round consistency

**Weight Acquisition Process:**
1. **Client Calculation**: Accumulate `batch_size` during training → `num_samples`
2. **Message Transport**: `(sample_size, model_parameters)` sent to server
3. **Server Extraction**: Extract sample sizes from message content
4. **Weight Computation**: `weight = local_sample_size / training_set_size`

**Message Interaction Timeline:**
```
Clients 1,2,3              Server
    |                       |
    | <-- model_para ------ | (Broadcast global model)
    |                       |
Local training...           |
    |                       |
    | ---- model_para ----> | (Send training results)
    |                    Buffer check
    |                    len(buffer) < min_num?
    |                       | Continue waiting...
    |                       |
    | ---- model_para ----> | (More client results)
    |                    Buffer check
    |                    len(buffer) >= min_num!
    |                    Execute aggregation
    |                    Update global model
    | <-- model_para ------ | (Broadcast new global model)
```

This synchronous design ensures consistency in federated learning through strict message buffering and threshold checking mechanisms, implementing true synchronous training coordination.
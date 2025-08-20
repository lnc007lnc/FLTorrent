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

## Common Debugging Tips

1. **Config Issues**: Check `cfg.freeze()` status and config merging order
2. **Data Issues**: Verify data path, splitting parameters, and dataset format
3. **Model Issues**: Ensure model architecture matches data dimensions
4. **Memory Issues**: Reduce batch size or model size in config
5. **Communication Issues**: Check server/client host/port settings for distributed mode
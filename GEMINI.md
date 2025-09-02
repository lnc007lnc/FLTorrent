# Gemini Code Assistant Context

This document provides a comprehensive overview of the FLTorrent project, its architecture, and usage instructions to be used as context for Gemini.

## Project Overview

FLTorrent is a decentralized federated learning (FL) framework that uses the BitTorrent protocol for peer-to-peer (P2P) model weight exchange. This approach eliminates the traditional centralized parameter server, creating a more robust, scalable, and fault-tolerant system. The project is built on top of Alibaba's [FederatedScope](https://github.com/alibaba/FederatedScope).

### Key Features

*   **Decentralized P2P Architecture:** No single point of failure.
*   **BitTorrent Weight Exchange:** Efficiently distributes model updates among peers.
*   **Partial Aggregation:** Can perform model aggregation even with incomplete model weight transfers.
*   **Flexible Network Topologies:** Supports various P2P network configurations (star, ring, mesh, etc.).
*   **Advanced Chunk Prioritization:** Uses multiple importance scoring algorithms (e.g., Magnitude, L2 Norm, SNIP, Fisher) to prioritize the transfer of critical model chunks.
*   **Device & Network Simulation:** Leverages Docker to simulate a wide range of heterogeneous devices (smartphones, IoT devices, etc.) and network conditions (bandwidth, latency, packet loss).
*   **Dynamic Resource Management:** Utilizes Ray for distributed process orchestration and performs automatic, fractional GPU allocation.

### Core Technologies

*   **Programming Language:** Python
*   **Machine Learning Framework:** PyTorch
*   **Distributed Computing:** Ray
*   **Containerization & Simulation:** Docker

## Building and Running

The project offers two main execution modes: a simple multi-process mode suitable for basic testing, and a powerful Ray and Docker-based mode for comprehensive simulations and large-scale experiments.

### 1. Simple Mode: Multi-Process Bash Script

This mode runs the server and clients as separate processes on a single machine without requiring Ray or Docker. It's the quickest way to test the basic functionality.

**To Run:**

1.  **Execute the script:**
    ```bash
    ./multi_process_fl_test_v2.sh
    ```

**What it does:**

*   Creates a `multi_process_test_v2` directory.
*   Generates YAML configuration files for a server and 3 clients inside `multi_process_test_v2/configs`.
*   Starts the server and client processes in the background.
*   Redirects all output to log files in `multi_process_test_v2/logs`.
*   Monitors the processes and cleans them up afterward.

### 2. Advanced Mode: Ray and Docker Orchestration

This is the most powerful mode, using Ray for distributed orchestration and Docker for containerizing each participant, enabling realistic device and network simulation.

**To Run:**

1.  **Prerequisites:**
    *   Ensure Docker is installed and running.
    *   The script will offer to auto-build the necessary Docker images on the first run.

2.  **Execute the script:**
    ```bash
    python run_ray.py
    ```

**What it does:**

*   **Centralized Configuration:** All experiment settings are managed within the `FLConfig` dataclass at the top of the `run_ray.py` script.
*   **Docker Environment:**
    *   Checks for the required Docker base image (`flv2:base`) and offers to build it if it's missing.
    *   Creates a dedicated Docker network for the FL experiment.
*   **Ray Cluster:** Initializes a Ray cluster to manage the distributed execution.
*   **Actor-Based Execution:**
    *   Starts the server and clients as Ray actors (`DockerFederatedScopeServer` and `DockerFederatedScopeClient`).
    *   Each actor runs within its own Docker container.
*   **Resource Allocation:** Automatically detects available GPUs and allocates fractional resources to client containers based on simulated device profiles.
*   **Simulation:** Applies network constraints (bandwidth, latency, etc.) to each container to simulate real-world conditions.
*   **Logging & Cleanup:** Manages logs and ensures all Docker containers and Ray resources are properly shut down after the experiment.

## Development Conventions

*   **Centralized Configuration:** The project makes heavy use of YAML files for configuration. The main execution scripts (`run_ray.py` and `multi_process_fl_test_v2.sh`) generate these files before starting the FL processes. For the Ray-based workflow, the `FLConfig` dataclass in `run_ray.py` serves as the single source of truth for all settings.
*   **Entry Points:** The primary entry point for the federated learning process is `federatedscope/main.py`, which is called by the orchestration scripts.
*   **Modularity:** The core logic is organized into modules within the `federatedscope` directory. New functionalities like the BitTorrent manager, chunk manager, and topology manager are implemented in their own respective files.
*   **Extensibility:** The project is designed to be extensible. New importance scoring methods, network topologies, and device profiles can be added by modifying the configuration and relevant manager classes.

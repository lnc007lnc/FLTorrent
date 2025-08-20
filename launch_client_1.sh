#!/bin/bash
# Launch FederatedScope Client 1
echo "Starting FederatedScope Client 1 on 127.0.0.1:50052..."
export PYTHONPATH="$PWD:$PYTHONPATH"
python federatedscope/main.py \
    --cfg scripts/distributed_scripts/distributed_configs/distributed_client_1.yaml \
    distribute.data_file 'fl_data/client_1_data' \
    distribute.server_host 127.0.0.1 \
    distribute.server_port 50051 \
    distribute.client_host 127.0.0.1 \
    distribute.client_port 50052

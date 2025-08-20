#!/bin/bash
# Launch FederatedScope Server
echo "Starting FederatedScope Server on 127.0.0.1:50051..."
export PYTHONPATH="$PWD:$PYTHONPATH"
python federatedscope/main.py \
    --cfg scripts/distributed_scripts/distributed_configs/distributed_server.yaml \
    distribute.data_file 'fl_data/server_data' \
    distribute.server_host 127.0.0.1 \
    distribute.server_port 50051

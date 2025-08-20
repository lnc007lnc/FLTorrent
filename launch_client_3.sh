#!/bin/bash
# Launch FederatedScope Client 3
echo "Starting FederatedScope Client 3 on 127.0.0.1:50054..."
python federatedscope/main.py \
    --cfg scripts/distributed_scripts/distributed_configs/distributed_client_3.yaml \
    distribute.data_file 'fl_data/client_3_data' \
    distribute.server_host 127.0.0.1 \
    distribute.server_port 50051 \
    distribute.client_host 127.0.0.1 \
    distribute.client_port 50054

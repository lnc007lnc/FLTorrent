#!/bin/bash
# Launch FederatedScope Client 2
echo "Starting FederatedScope Client 2 on 127.0.0.1:50053..."
python federatedscope/main.py \
    --cfg scripts/distributed_scripts/distributed_configs/distributed_client_2.yaml \
    distribute.data_file 'fl_data/client_2_data' \
    distribute.server_host 127.0.0.1 \
    distribute.server_port 50051 \
    distribute.client_host 127.0.0.1 \
    distribute.client_port 50053

# Anonymous Artifact for EuroSys 2026 Submission

This repository contains the anonymized implementation accompanying our
EuroSys 2026 submission. It is intended as a **review artifact** so that
reviewers can inspect the implementation and run a representative
end-to-end example. It is **not** a complete artifact-evaluation package
and does not include the raw experiment outputs reported in the paper.

## Overview

The artifact implements a deadline-bounded swarming substrate for
decentralized federated learning. The runtime disseminates each round's
model updates as fixed-size chunks over a peer overlay using BitTorrent
semantics (HAVE / BITFIELD / REQUEST / PIECE messages); a deadline
interrupts the exchange even when peers have not received every chunk,
and the training system aggregates over the reconstructable subset.

The artifact exposes the main mechanisms evaluated in the paper:
importance-aware chunk scheduling and post-aggregation compensation for
partial reception. It also includes an owner-only-serving switch used as
an ablation control to disable same-round re-serving.

## Repository layout

```text
README.md, LICENSE, .gitignore
configs/
    cadence_review.yaml       # compact end-to-end review configuration
environment/
    requirements-torch1.10.txt  # pip dependencies
federatedscope/
    main.py                     # CLI entry point
    register.py                 # plugin registry (datasets, models, trainers)
    __init__.py
    core/                       # Cadence runtime + FederatedScope core
    cv/                         # CV models / datasets used in paper experiments
    contrib/                    # registry-pattern extensions used by configs
```

The included YAML file is a compact review configuration for exercising
the implementation. It is not the full paper-scale experimental setup.

## What is not included

This repository does not include raw logs, full-scale distributed
experiment outputs, SLURM scripts, cloud deployment files, backup files,
or paper PDFs. These files were removed to preserve anonymity and keep
the review artifact focused on implementation inspection.

The repository is therefore intended for code inspection and a
representative end-to-end run, not for full artifact evaluation.

## Relevant code paths

| Component (paper) | Code location |
|---|---|
| BitTorrent dissemination runtime | `federatedscope/core/bittorrent_manager.py` |
| Chunk storage and reconstruction | `federatedscope/core/chunk_manager.py` |
| Streaming chunk channel (gRPC) | `federatedscope/core/streaming_channel_manager.py` |
| Reliable transport layer | `federatedscope/core/reliable_transport.py` |
| Worker integration (client / server) | `federatedscope/core/workers/{client,server}.py` |
| Round coordination loop | `federatedscope/core/fed_runner.py` |
| Per-round metadata / CPU / re-serving stats | `federatedscope/core/overhead_stats.py` |
| Sparsification (CHOCO / GossipFL baselines) | `federatedscope/core/compression/sparsification.py` |
| BitTorrent / topology config schemas | `federatedscope/core/configs/cfg_bittorrent.py`, `cfg_topology.py` |
| Topology construction | `federatedscope/core/topology_manager.py` |
| CV models and datasets used in experiments | `federatedscope/cv/` |

Several FederatedScope upstream subsystems (graph FL, NLP, vertical FL,
contrastive learning, matrix factorization, autotune, attack) have been
removed; they are not part of this paper's contribution and are not
exercised by the included config. The remaining `core/`, `cv/`, and
`contrib/` subtrees together form a self-contained import graph.

## Quick start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r environment/requirements-torch1.10.txt

# Run the included single-machine review configuration.
# Standalone mode simulates the Cadence dissemination and aggregation
# loop on one host.
python federatedscope/main.py --cfg configs/cadence_review.yaml
```

The review config uses a compact CIFAR-10 setup intended for a
lightweight single-machine run. GPU acceleration is recommended. The
intent is to validate that the Cadence runtime starts, chunk
scheduling fires, and aggregation completes; it is not a reproduction of
the paper-scale numbers.

To enable per-round bookkeeping (HAVE / BITFIELD bytes, scoring and
compensation CPU time, served-own vs received-then-re-served chunks),
set in the YAML config:

```yaml
bittorrent:
  collect_overhead_stats: True
  collect_isolation_stats: True
```

Per-round JSON Lines records are then written under
`<outdir>/bittorrent_logs/overhead/client_<id>.jsonl`.

## Reviewer roadmap

To inspect the main implementation, start from:

1. `federatedscope/core/bittorrent_manager.py` — the dissemination
   runtime: HAVE/BITFIELD broadcast, REQUEST/PIECE handling, the
   per-round upload worker, the owner-only-serving switch, and the
   per-round-deadline boundary.
2. `federatedscope/core/chunk_manager.py` — chunk slicing, local
   storage, and per-round reconstruction at the deadline.
3. `federatedscope/core/workers/client.py` — wiring of the BT manager
   into the FedAvg round loop, including importance computation and
   the post-aggregation compensation step.
4. `federatedscope/core/overhead_stats.py` — per-round instrumentation
   used to produce the metadata-byte and CPU-time numbers reported in
   the paper.
5. `configs/cadence_review.yaml` — the single end-to-end review
   configuration.
6. Then run the quick start above.

## Notes on anonymity

Submission-author-identifying information, local paths, raw experiment
outputs (`experiments_output/`, `slurm_logs/`, `*.log`, `*.csv`,
`*.jsonl` result files), backup files, and prior-version paper PDFs have
been removed from this snapshot. The git history of this branch consists
of a single anonymous commit.

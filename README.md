# FLTorrent (SIGCOMM 2026 Anonymous Submission Snapshot)

This repository is an anonymized, compact code snapshot for paper review.
It keeps the core implementation and reproducible experiment configs, while
excluding large artifacts and internal operational files.

## Included

- Core framework and FLTorrent logic: `federatedscope/`
- Experiment configs used in this snapshot: `configs/`, `experiments.yaml`
- Main runners: `run_ray.py`, `run_ray_hpc.py`, `run_fl_wrapper.sh`
- Reproduction helper scripts: `scripts/run_exp*.slurm`, `slurm_submit.sh`
- Environment requirements: `environment/requirements-*.txt`
- Historical draft paper (unpublished v1): `fltorrent.pdf`

## Excluded (for anonymity and clarity)

- Experiment outputs, logs, checkpoints, temporary chunk caches
- Non-essential benchmark/tutorial/material directories
- Internal deployment and infrastructure files
- Extra figures and intermediate analysis artifacts

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r environment/requirements-torch1.10.txt
```

## Run a Single Config

```bash
python federatedscope/main.py --cfg configs/fltorrent_review.yaml
```

## Batch Submit (SLURM)

```bash
python run_ray_hpc.py --batch experiments.yaml
```

## Notes

- Paths in configs are repository-relative where possible.
- If a dataset path is required in your environment, adjust `DATA_ROOT` in the
  corresponding config file.

# FLTorrent (SIGCOMM 2026 Anonymous Submission Snapshot)

This repository is a minimal anonymous review snapshot.
It contains only the FLTorrent core implementation and one runnable config.

## Included

- Core code: `federatedscope/`
- Single review config: `configs/fltorrent_review.yaml`
- Dependency spec: `environment/requirements-torch1.10.txt`
- Unpublished v1 draft paper referenced by the submission: `fltorrent.pdf`

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r environment/requirements-torch1.10.txt
```

## Run

```bash
python federatedscope/main.py --cfg configs/fltorrent_review.yaml
```

## Notes

- This snapshot intentionally excludes HPC orchestration and internal experiment scripts.
- If needed in your environment, adjust dataset path settings in `configs/fltorrent_review.yaml`.

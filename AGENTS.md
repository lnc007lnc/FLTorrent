# Repository Guidelines

## Project Structure & Module Organization
- `federatedscope/`: Core FL code and entrypoint `main.py`.
- `scripts/`: Reproduction and example scripts (see `scripts/README.md`).
- `configs/`: Ready-to-run configs (e.g., `configs/docker_simulation.yaml`).
- `environment/`: Dependency specs and helper install scripts.
- `data/`, `exp/`, `logs/`: Local datasets, experiment outputs, and logs.
- Tests: top-level `test_*.py` files and `tests/` folder.

## Build, Test, and Development Commands
- Create env: `python3 -m venv .venv && source .venv/bin/activate`
- Install deps: `pip install -r environment/requirements-torch1.10.txt`
- Optional extras (conda): `bash environment/extra_dependencies_torch1.10-application.sh`
- Lint: `flake8 .`
- Format: `yapf -ir federatedscope scripts run_ray.py`
- Tests: `pytest -q`
- Run (vanilla): `python federatedscope/main.py --cfg scripts/example_configs/femnist.yaml`
- Run (Ray orchestration): `python run_ray.py`

## Coding Style & Naming Conventions
- Python, 4-space indentation; max line length 79 (see `.flake8`).
- Strings use double quotes; avoid wildcard imports.
- Names: `snake_case` for functions/variables/modules, `CamelCase` for classes, constants in `UPPER_SNAKE_CASE`.
- Keep functions small; prefer explicit over implicit; add docstrings for public APIs.
- Tools: `flake8` for linting, `yapf` (PEP8-based) for formatting; enable `pre-commit`:
  - `pip install pre-commit && pre-commit install && pre-commit run -a`

## Testing Guidelines
- Framework: `pytest` with test files named `test_*.py`.
- Place new tests near related modules or in `tests/`.
- Aim for fast, deterministic unit tests; mock external I/O.
- Run full suite locally: `PYTHONPATH=. pytest -q`.

## Commit & Pull Request Guidelines
- Commits: follow Conventional Commits.
  - Examples: `feat(core): add krum aggregator`, `fix(cache): handle missing chunk`, `test(db): add migration case`.
  - Use imperative mood; keep subject ≤ 72 chars.
- PRs: include clear description, linked issues (`Fixes #123`), reproduction steps, and before/after metrics or logs.
- Requirements: passing tests and lint, updated docs/configs when behavior changes.

## Security & Configuration Tips
- Never commit secrets or personal data; keep datasets under `data/` (gitignored).
- GPU/CPU settings and networking are config-driven (see `configs/docker_simulation.yaml`).
- For distributed runs, start with small `client_num` locally and validate logs in `logs/` before scaling.

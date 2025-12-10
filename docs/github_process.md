# GitHub Process — Phase-1

## Purpose
Track code, data configs, and docs with clean history, reviews, and stable checkpoints.

## Branching
- Work on feature branches (e.g., `feature/source-compiler-v1`).
- Rebase on `main` before pushing (avoid noisy merges when possible).

## Pull Requests
- Title template: `Phase-1: <scope>`  
- Description: what changed, why, validation (counts/ranges), risk
- Required checks before PR:  
  - `check_src_counts.py` shows expected counts  
  - CDM build succeeds  
  - Quarantine reviewed (only expected rejects)

## Tags / Releases
- Tag stable baselines (e.g., `v1-phase1`).
- Use tags for rollbacks, sharing, and CI anchors.

## Large Files & .gitignore
- Do **not** commit virtualenv, compiled libs (`.pyd/.exe`), or big binaries.
- Use `.gitignore` entries for: envs (`finrep_env/`), `data/` (if any local dumps), logs, notebooks, and BI artifacts (`*.pbix`).

## Security & Scope
- No PII in repo.  
- Only schemas/configs/scripts & synthetic fixtures.  
- Real client files are ingested locally via compiler; not committed.

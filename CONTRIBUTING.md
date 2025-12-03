# CONTRIBUTING (Phase 1)

## Environment
- Windows CMD only.
- Python 3.11.x in `C:\Users\Dick\pyproj_finrep\finrep_env`.

## Ground rules
1. Don’t rename functions/vars used by downstream steps.
2. Prefer config/script changes over schema edits.
3. Keep loads idempotent (truncate relevant `SRC_*` first when re-ingesting).
4. No large binaries in Git (envs, wheels, .pyd, .exe).

## Git (feature → PR) — template for later
    cd C:\Users\Dick\pyproj_finrep
    git status
    git fetch origin
    git checkout -B feature/your-change origin/main
    git add -A
    git commit -m "Your change: what/why"
    git pull --rebase origin main
    git push -u origin feature/your-change

## Checks before PR
- `python check_src_counts.py` shows expected non-zero SRC counts.
- CDM built (`python load_cdm_phase1.py`) with sane date ranges.
- Quarantine only contains justified rejects.

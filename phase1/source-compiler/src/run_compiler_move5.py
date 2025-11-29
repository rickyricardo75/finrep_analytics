from pathlib import Path
from source_compiler import run_compiler, BASE

if __name__ == "__main__":
    cfg = BASE / r"source-compiler\config\source_generic.yaml"
    totals = run_compiler(cfg)
    print("Done. Totals:", totals)

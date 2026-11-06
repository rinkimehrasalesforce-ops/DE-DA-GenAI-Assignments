"""Make the quarantine-rules module importable: it lives next to the pipeline
source in src/pipeline/, which the bundle ships as pipeline libraries."""

import sys
from pathlib import Path

SRC_PIPELINE = Path(__file__).resolve().parents[1] / "src" / "pipeline"
if str(SRC_PIPELINE) not in sys.path:
    sys.path.insert(0, str(SRC_PIPELINE))

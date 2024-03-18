import sys
from pathlib import Path

KUZU_ROOT = Path(__file__).parent.parent.parent.parent

if sys.platform == "win32":
    # \ in paths is not supported by kuzu's parser
    KUZU_ROOT = str(KUZU_ROOT).replace("\\", "/")

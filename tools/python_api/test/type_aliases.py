import sys

from kuzu import Connection, Database

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

ConnDB: TypeAlias = tuple[Connection, Database]

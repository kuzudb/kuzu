from enum import Enum


class Type(Enum):
    """The type of a value in the database."""
    BOOL = "BOOL"
    INT64 = "INT64"
    DOUBLE = "DOUBLE"
    STRING = "STRING"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    INTERVAL = "INTERVAL"
    LIST = "LIST"
    NODE = "NODE"
    REL = "REL"
    NODE_ID = "NODE_ID"

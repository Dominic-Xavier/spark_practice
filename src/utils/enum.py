from enum import Enum

class WriteMode(Enum):
    OVERWRITE = "overwrite"
    APPEND = "append"
    IGNORE = "ignore"
    ERROR_IF_EXISTS = "errorifexists"
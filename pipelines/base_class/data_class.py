from enum import Enum


class WriteMode(Enum):
    DATA_LAKE_PATH = "datalakepath"
    UC_EXTERNAL_TABLE = "ucexternaltable"
    UC_MANAGED_TABLE = "ucmanagedtable"


class TriggerMode(Enum):
    """
    If batch it should use the Trigger.Available() API. Otherwise no trigger should be used.
    """

    CONTINUOUS = "continuous"
    BATCH = "batch"

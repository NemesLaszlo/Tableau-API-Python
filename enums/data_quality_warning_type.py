from enum import Enum


class DataQualityWarningType(Enum):
    DEPRECATED = "DEPRECATED"
    WARNING = "WARNING"
    STALE = "STALE"
    SENSITIVE_DATA = "SENSITIVE_DATA"
    MAINTENANCE = "MAINTENANCE"
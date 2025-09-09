from enum import Enum


class MetadataContentType(Enum):
    database = "database"
    table = "table"
    datasource = "datasource"
    flow = "flow"
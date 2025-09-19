"""
Tableau API Client - A Python library for interacting with Tableau Server REST API.

This package provides a comprehensive Python client for the Tableau Server REST API,
including support for authentication, data sources, workbooks, users, groups, projects,
and more.

Example usage:
    from tableau_api_client import TableauApiClient
    
    client = TableauApiClient('https://my-tableau-server.com', api_version='3.19')
    # Use the client to interact with Tableau Server
"""

# Main API Client
from .tableau_api_client import TableauApiClient

# All model types from the generated API models
from .models import *

# Client modules
from .clients import (
    TableauAuthenticationClient,
    TableauDatasourcesClient,
    TableauFavoritesClient,
    TableauFlowsClient,
    TableauJobsTasksSchedulesClient,
    TableauPermissionsClient,
    TableauProjectsClient,
    TableauPublishingClient,
    TableauRevisionsClient,
    TableauServerInfoClient,
    TableauSitesClient,
    TableauSubscriptionsClient,
    TableauUsersGroupsClient,
    TableauWorkbooksViewsClient
)

# Enums
from .enums import (
    DatasourceFileType,
    Orientation,
    PageOrientation,
    PageType,
    TableauImageWidth,
    WorkbookFileType,
)

# Exceptions
from .exceptions import (
    TableauApiVersionException,
    TableauOnlineNotSupportedException,
    TableauRequestException,
)

# Attributes/Decorators
from .attributes import (
    ApiVersionAttribute,
    OnPremiseOnlyAttribute,
)

# Version info
__version__ = "1.0.0"
__author__ = "Laszlo Nemes"
__email__ = "wow.laszlo@gmail.com"

# Main exports - what users get when they do "from tableau_api_client import *"
__all__ = [
    # Main client
    "TableauApiClient",
    
    # Client modules
    "TableauAuthenticationClient",
    "TableauDatasourcesClient", 
    "TableauFavoritesClient",
    "TableauFlowsClient",
    "TableauJobsTasksSchedulesClient",
    "TableauPermissionsClient",
    "TableauProjectsClient", 
    "TableauPublishingClient",
    "TableauRevisionsClient",
    "TableauServerInfoClient",
    "TableauSitesClient",
    "TableauSubscriptionsClient",
    "TableauUsersGroupsClient",
    "TableauWorkbooksViewsClient",
    
    # Enums
    "DatasourceFileType",
    "Orientation", 
    "PageOrientation",
    "PageType",
    "TableauImageWidth",
    "WorkbookFileType",
    
    # Exceptions
    "TableauApiVersionException",
    "TableauOnlineNotSupportedException",
    "TableauRequestException",
    
    # Attributes/Decorators
    "ApiVersionAttribute",
    "OnPremiseOnlyAttribute",
]

from .models import __all__ as models_all
__all__.extend(models_all)
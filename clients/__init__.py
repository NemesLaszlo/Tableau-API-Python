"""
Tableau API Client modules.

This package contains all the client classes for interacting with different
aspects of the Tableau Server REST API.
"""

from .tableau_authentication import TableauAuthenticationClient
from .tableau_datasources import TableauDatasourcesClient  
from .tableau_favorites import TableauFavoritesClient
from .tableau_flows import TableauFlowsClient
from .tableau_jobs_tasks_schedules import TableauJobsTasksSchedulesClient
from .tableau_permissions import TableauPermissionsClient
from .tableau_projects import TableauProjectsClient
from .tableau_revisions import TableauRevisionsClient

__all__ = [
    'TableauAuthenticationClient',
    'TableauDatasourcesClient',
    'TableauFavoritesClient', 
    'TableauFlowsClient',
    'TableauJobsTasksSchedulesClient',
    'TableauPermissionsClient',
    'TableauProjectsClient',
    'TableauRevisionsClient'
]
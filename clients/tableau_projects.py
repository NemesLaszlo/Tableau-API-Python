from typing import Optional, Tuple, List, TYPE_CHECKING
from models.ts_api import (
    ProjectType, 
    ProjectListType, 
    PaginationType,
    ProjectTypeContentPermissions
)
from models.tableau_session import TableauSession
from attributes.api_version_attribute import ApiVersionAttribute

if TYPE_CHECKING:
    from tableau_api_client import TableauApiClient

class TableauProjectsClient:
    """Client for Tableau Server project operations"""
    
    def __init__(self, api_client: "TableauApiClient"):
        """
        Initialize projects client
        
        Args:
            api_client: Instance of TableauApiClient
        """
        self._api_client = api_client
    
    @ApiVersionAttribute(1, 0)
    def create_project(self, 
                      session: TableauSession,
                      project_name: str,
                      description: Optional[str] = None,
                      content_permissions: ProjectTypeContentPermissions = ProjectTypeContentPermissions.MANAGED_BY_OWNER,
                      parent_project_id: Optional[str] = None,
                      publish_samples: bool = False) -> ProjectType:
        """
        Creates a project. Available in all versions.
        
        Args:
            session: TableauSession for authentication
            project_name: The name to assign to the project
            description: (Optional) A description for the project
            content_permissions: (Optional) Specify LockedToProject to lock permissions so that users cannot 
                               overwrite the default permissions set for the project, or specify ManagedByOwner 
                               to allow users to manage permissions for content that they own. Default: ManagedByOwner
            parent_project_id: (Optional) The identifier of the parent project. Use this option to create project hierarchies
            publish_samples: (Optional) A Boolean value that specifies whether to publish the sample workbooks 
                           provided by Tableau to the project. Default: False
            
        Returns:
            ProjectType: The created project
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("project_name", project_name)
        )
        
        # Create project object
        project = ProjectType()
        project.name = project_name
        project.description = description
        project.parent_project_id = parent_project_id
        project.content_permissions = content_permissions
        
        # Build URI with query parameters
        query_params = [("publishSamples", str(publish_samples).lower())]
        uri = self._api_client.build_uri(
            f"sites/{session.site_id}/projects", 
            *query_params
        )
        
        request_body = self._api_client.get_object_as_request_content(project)
        
        response_content = self._api_client.api_request(
            uri, "POST", 201, session=session, body=request_body
        )
        
        return self._api_client.get_response_as_object(response_content, ProjectType)
    
    @ApiVersionAttribute(2, 0)
    def query_projects(self,
                      session: TableauSession,
                      page_size: int = 100,
                      page_number: int = 1,
                      filter_expression: Optional[str] = None,
                      sort_expression: Optional[str] = None) -> Tuple[PaginationType, List[ProjectType]]:
        """
        Queries projects. Available in versions 2.0+.
        
        Args:
            session: TableauSession for authentication
            page_size: (Optional) The number of items to return in one response. 
                      The minimum is 1. The maximum is 1000. The default is 100
            page_number: (Optional) The offset for paging. The default is 1
            filter_expression: (Optional) An expression that lets you specify a subset of projects to return.
                             You can filter on predefined fields such as name, ownerName, and parentProjectId.
                             You can include multiple filter expressions
            sort_expression: (Optional) An expression that lets you specify the order in which project 
                           information is returned. If you do not specify a sort expression, the sort 
                           order of the information that's returned is undefined
            
        Returns:
            Tuple[PaginationType, List[ProjectType]]: Pagination info and list of projects
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing or out of range
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(("session", session))
        self._api_client.check_parameters_between(
            ("page_size", page_size, 1, 1000),
            ("page_number", page_number, 1, float('inf'))
        )
        
        # Build query parameters
        query_params = [
            ("pageSize", str(page_size)),
            ("pageNumber", str(page_number))
        ]
        
        if filter_expression:
            query_params.append(("filter", filter_expression))
        
        if sort_expression:
            query_params.append(("sort", sort_expression))
        
        uri = self._api_client.build_uri(
            f"sites/{session.site_id}/projects",
            *query_params
        )
        
        response_content = self._api_client.api_request(
            uri, "GET", 200, session=session, body=None
        )
        
        pagination, project_list = self._api_client.get_response_as_objects(
            response_content, PaginationType, ProjectListType
        )
        
        projects = project_list.project if project_list and project_list.project else []
        return pagination, projects
    
    @ApiVersionAttribute(1, 0)
    def update_project(self,
                      session: TableauSession,
                      project_id: str,
                      new_project_name: Optional[str] = None,
                      new_description: Optional[str] = None,
                      new_content_permissions: Optional[ProjectTypeContentPermissions] = None,
                      new_parent_project_id: Optional[str] = None,
                      publish_samples: bool = False) -> ProjectType:
        """
        Updates a project. Available in all versions.
        
        Args:
            session: TableauSession for authentication
            project_id: The ID of the project to update
            new_project_name: (Optional) The new name for the project
            new_description: (Optional) The new description for the project
            new_content_permissions: (Optional) Specify LockedToProject to lock permissions so that users cannot 
                                   overwrite the default permissions set for the project, or specify ManagedByOwner 
                                   to allow users to manage permissions for content that they own
            new_parent_project_id: (Optional) The new identifier of the parent project. Use this option to 
                                 create or change project hierarchies
            publish_samples: (Optional) A Boolean value that specifies whether to publish the sample workbooks 
                           provided by Tableau to the project. Default: False
            
        Returns:
            ProjectType: The updated project
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("project_id", project_id)
        )
        
        # Create project object with updates
        project = ProjectType()
        project.name = new_project_name
        project.description = new_description
        project.parent_project_id = new_parent_project_id
        
        # Set content permissions if provided, otherwise use default
        if new_content_permissions is not None:
            project.content_permissions = new_content_permissions
        else:
            project.content_permissions = ProjectTypeContentPermissions.MANAGED_BY_OWNER
        
        # Build URI with query parameters
        query_params = [("publishSamples", str(publish_samples).lower())]
        uri = self._api_client.build_uri(
            f"sites/{session.site_id}/projects/{project_id}",
            *query_params
        )
        
        request_body = self._api_client.get_object_as_request_content(project)
        
        response_content = self._api_client.api_request(
            uri, "PUT", 200, session=session, body=request_body
        )
        
        return self._api_client.get_response_as_object(response_content, ProjectType)
    
    @ApiVersionAttribute(1, 0)
    def delete_project(self,
                      session: TableauSession,
                      project_id: str) -> None:
        """
        Deletes a project. Available in all versions.
        
        Args:
            session: TableauSession for authentication
            project_id: The ID of the project to delete
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("project_id", project_id)
        )
        
        uri = self._api_client.build_uri(f"sites/{session.site_id}/projects/{project_id}")
        
        self._api_client.api_request(
            uri, "DELETE", 204, session=session, body=None
        )
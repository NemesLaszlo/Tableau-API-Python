from typing import Optional, List, TYPE_CHECKING
from client_registry import ClientRegistry
from models.ts_api import (
    PermissionsType, DataSourceType, ProjectType, WorkbookType, ViewType,
    GranteeCapabilitiesType, TaskType, TaskExtractRefreshType,
    CapabilityTypeName, CapabilityTypeMode
)
from models.tableau_session import TableauSession
from attributes.api_version_attribute import ApiVersionAttribute
from attributes.on_premise_only_attribute import OnPremiseOnlyAttribute

if TYPE_CHECKING:
    from tableau_api_client import TableauApiClient


@ClientRegistry.register("permissions")
class TableauPermissionsClient:
    """Client for Tableau Server permissions operations"""
    
    def __init__(self, api_client: "TableauApiClient"):
        """
        Initialize permissions client
        
        Args:
            api_client: Instance of TableauApiClient
        """
        self._api_client = api_client

    @ApiVersionAttribute(2, 0)
    def add_data_source_permissions(self, 
                                  session: TableauSession, 
                                  datasource_id: str, 
                                  grantee_capabilities: List[GranteeCapabilitiesType]) -> PermissionsType:
        """
        Adds permissions to the specified data source for a Tableau Server user or group. 
        You can specify multiple sets of permissions using one call. Available in all versions.
        
        Args:
            session: TableauSession object
            datasource_id: The ID of the datasource
            grantee_capabilities: The permissions to assign. Permissions can either be for a user or group
            
        Returns:
            PermissionsType: The updated permissions
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session), 
            ("datasource_id", datasource_id), 
            ("grantee_capabilities", grantee_capabilities)
        )
        
        # Create permissions object
        permissions = PermissionsType()
        permissions.datasource = DataSourceType()
        permissions.datasource.id = datasource_id
        permissions.grantee_capabilities = grantee_capabilities
        
        # Build URI and make request
        uri = self._api_client._build_uri(f"sites/{session.site_id}/datasources/{datasource_id}/permissions")
        request_body = self._api_client._get_object_as_request_content(permissions)
        
        response_content = self._api_client._api_request(
            uri, "PUT", 200, session=session, body=request_body
        )
        
        return self._api_client._get_response_as_object(response_content, PermissionsType)

    @ApiVersionAttribute(2, 0)
    def add_project_permissions(self, 
                              session: TableauSession, 
                              project_id: str, 
                              grantee_capabilities: List[GranteeCapabilitiesType]) -> PermissionsType:
        """
        Adds permissions to the specified project for a Tableau Server user or group. 
        You can specify multiple sets of permissions using one call. Available in all versions.
        
        Args:
            session: TableauSession object
            project_id: The ID of the project
            grantee_capabilities: The permissions to assign. Permissions can either be for a user or group
            
        Returns:
            PermissionsType: The updated permissions
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session), 
            ("project_id", project_id), 
            ("grantee_capabilities", grantee_capabilities)
        )
        
        # Create permissions object
        permissions = PermissionsType()
        permissions.project = ProjectType()
        permissions.project.id = project_id
        permissions.grantee_capabilities = grantee_capabilities
        
        # Build URI and make request
        uri = self._api_client._build_uri(f"sites/{session.site_id}/projects/{project_id}/permissions")
        request_body = self._api_client._get_object_as_request_content(permissions)
        
        response_content = self._api_client._api_request(
            uri, "PUT", 200, session=session, body=request_body
        )
        
        return self._api_client._get_response_as_object(response_content, PermissionsType)

    @ApiVersionAttribute(2, 0)
    def add_default_permissions_for_workbooks(self, 
                                            session: TableauSession, 
                                            project_id: str, 
                                            grantee_capabilities: List[GranteeCapabilitiesType]) -> PermissionsType:
        """
        Adds permissions to the specified project that will be applied by default to new workbooks 
        as they are added to the project. Available in all versions.
        
        Args:
            session: TableauSession object
            project_id: The ID of the project
            grantee_capabilities: The permissions to assign. Permissions can either be for a user or group
            
        Returns:
            PermissionsType: The updated permissions
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session), 
            ("project_id", project_id), 
            ("grantee_capabilities", grantee_capabilities)
        )
        
        # Create permissions object
        permissions = PermissionsType()
        permissions.grantee_capabilities = grantee_capabilities
        
        # Build URI and make request
        uri = self._api_client._build_uri(f"sites/{session.site_id}/projects/{project_id}/default-permissions/workbooks")
        request_body = self._api_client._get_object_as_request_content(permissions)
        
        response_content = self._api_client._api_request(
            uri, "PUT", 200, session=session, body=request_body
        )
        
        return self._api_client._get_response_as_object(response_content, PermissionsType)

    @ApiVersionAttribute(2, 0)
    def add_default_permissions_for_data_sources(self, 
                                                session: TableauSession, 
                                                project_id: str, 
                                                grantee_capabilities: List[GranteeCapabilitiesType]) -> PermissionsType:
        """
        Adds permissions to the specified project that will be applied by default to new DataSources 
        as they are added to the project. Available in all versions.
        
        Args:
            session: TableauSession object
            project_id: The ID of the project
            grantee_capabilities: The permissions to assign. Permissions can either be for a user or group
            
        Returns:
            PermissionsType: The updated permissions
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session), 
            ("project_id", project_id), 
            ("grantee_capabilities", grantee_capabilities)
        )
        
        # Create permissions object
        permissions = PermissionsType()
        permissions.grantee_capabilities = grantee_capabilities
        
        # Build URI and make request
        uri = self._api_client._build_uri(f"sites/{session.site_id}/projects/{project_id}/default-permissions/datasources")
        request_body = self._api_client._get_object_as_request_content(permissions)
        
        response_content = self._api_client._api_request(
            uri, "PUT", 200, session=session, body=request_body
        )
        
        return self._api_client._get_response_as_object(response_content, PermissionsType)

    @ApiVersionAttribute(2, 0)
    def add_workbook_permissions(self, 
                               session: TableauSession, 
                               workbook_id: str, 
                               grantee_capabilities: List[GranteeCapabilitiesType]) -> PermissionsType:
        """
        Adds permissions to the specified workbook for a Tableau Server user or group. 
        You can specify multiple sets of permissions using one call. Available in all versions.
        
        Args:
            session: TableauSession object
            workbook_id: The ID of the workbook
            grantee_capabilities: The permissions to assign. Permissions can either be for a user or group
            
        Returns:
            PermissionsType: The updated permissions
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session), 
            ("workbook_id", workbook_id), 
            ("grantee_capabilities", grantee_capabilities)
        )
        
        # Create permissions object
        permissions = PermissionsType()
        permissions.workbook = WorkbookType()
        permissions.workbook.id = workbook_id
        permissions.grantee_capabilities = grantee_capabilities
        
        # Build URI and make request
        uri = self._api_client._build_uri(f"sites/{session.site_id}/workbooks/{workbook_id}/permissions")
        request_body = self._api_client._get_object_as_request_content(permissions)
        
        response_content = self._api_client._api_request(
            uri, "PUT", 200, session=session, body=request_body
        )
        
        return self._api_client._get_response_as_object(response_content, PermissionsType)

    @ApiVersionAttribute(2, 8)
    def add_workbook_to_schedule(self, 
                               session: TableauSession, 
                               workbook_id: str, 
                               schedule_id: str, 
                               grantee_capabilities: List[GranteeCapabilitiesType]) -> PermissionsType:
        """
        Adds a task to refresh a workbook to an existing schedule. Versions 2.8+
        
        Args:
            session: TableauSession object
            workbook_id: The ID of the workbook
            schedule_id: The ID of the schedule
            grantee_capabilities: The permissions to assign. Permissions can either be for a user or group
            
        Returns:
            PermissionsType: The updated permissions
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session), 
            ("workbook_id", workbook_id), 
            ("schedule_id", schedule_id), 
            ("grantee_capabilities", grantee_capabilities)
        )
        
        # Create task object
        task = TaskType()
        task.extract_refresh = TaskExtractRefreshType()
        task.extract_refresh.workbook = WorkbookType()
        task.extract_refresh.workbook.id = workbook_id
        
        # Build URI and make request
        uri = self._api_client._build_uri(f"sites/{session.site_id}/schedules/{schedule_id}/workbooks")
        request_body = self._api_client._get_object_as_request_content(task)
        
        response_content = self._api_client._api_request(
            uri, "PUT", 200, session=session, body=request_body
        )
        
        return self._api_client._get_response_as_object(response_content, PermissionsType)

    # Query methods
    @ApiVersionAttribute(2, 0)
    def query_data_source_permissions(self, 
                                    session: TableauSession, 
                                    datasource_id: str) -> PermissionsType:
        """
        Returns a list of permissions for a specific data source. Available in all versions.
        
        Args:
            session: TableauSession object
            datasource_id: The ID of the datasource
            
        Returns:
            PermissionsType: The permissions for the data source
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session), 
            ("datasource_id", datasource_id)
        )
        
        # Build URI and make request
        uri = self._api_client._build_uri(f"sites/{session.site_id}/datasources/{datasource_id}/permissions")
        
        response_content = self._api_client._api_request(
            uri, "GET", 200, session=session
        )
        
        return self._api_client._get_response_as_object(response_content, PermissionsType)

    @ApiVersionAttribute(2, 0)
    def query_project_permissions(self, 
                                session: TableauSession, 
                                project_id: str) -> PermissionsType:
        """
        Returns a list of permissions for a specific project. Available in all versions.
        
        Args:
            session: TableauSession object
            project_id: The ID of the project
            
        Returns:
            PermissionsType: The permissions for the project
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session), 
            ("project_id", project_id)
        )
        
        # Build URI and make request
        uri = self._api_client._build_uri(f"sites/{session.site_id}/projects/{project_id}/permissions")
        
        response_content = self._api_client._api_request(
            uri, "GET", 200, session=session
        )
        
        return self._api_client._get_response_as_object(response_content, PermissionsType)

    @ApiVersionAttribute(2, 0)
    def query_default_permissions_for_workbooks(self, 
                                              session: TableauSession, 
                                              project_id: str) -> PermissionsType:
        """
        Returns a list of default permissions for workbooks of a specific project. Available in all versions.
        
        Args:
            session: TableauSession object
            project_id: The ID of the project (Note: C# version uses workbook_id but endpoint suggests project_id)
            
        Returns:
            PermissionsType: The default permissions for workbooks in the project
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session), 
            ("project_id", project_id)
        )
        
        # Build URI and make request
        uri = self._api_client._build_uri(f"sites/{session.site_id}/projects/{project_id}/default-permissions/workbooks")
        
        response_content = self._api_client._api_request(
            uri, "GET", 200, session=session
        )
        
        return self._api_client._get_response_as_object(response_content, PermissionsType)

    @ApiVersionAttribute(2, 0)
    def query_default_permissions_for_data_sources(self, 
                                                  session: TableauSession, 
                                                  project_id: str) -> PermissionsType:
        """
        Returns a list of default permissions for data sources of a specific project. Available in all versions.
        
        Args:
            session: TableauSession object
            project_id: The ID of the project (Note: C# version uses datasource_id but endpoint suggests project_id)
            
        Returns:
            PermissionsType: The default permissions for data sources in the project
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session), 
            ("project_id", project_id)
        )
        
        # Build URI and make request
        uri = self._api_client._build_uri(f"sites/{session.site_id}/projects/{project_id}/default-permissions/datasources")
        
        response_content = self._api_client._api_request(
            uri, "GET", 200, session=session
        )
        
        return self._api_client._get_response_as_object(response_content, PermissionsType)

    @ApiVersionAttribute(3, 2)
    def query_view_permissions(self, 
                             session: TableauSession, 
                             view_id: str) -> PermissionsType:
        """
        Returns a list of permissions for a specific view.
        
        Args:
            session: TableauSession object
            view_id: The ID of the view to get permissions for
            
        Returns:
            PermissionsType: The permissions for the view
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session), 
            ("view_id", view_id)
        )
        
        # Build URI and make request
        uri = self._api_client._build_uri(f"sites/{session.site_id}/views/{view_id}/permissions")
        
        response_content = self._api_client._api_request(
            uri, "GET", 200, session=session
        )
        
        return self._api_client._get_response_as_object(response_content, PermissionsType)

    @ApiVersionAttribute(2, 0)
    def query_workbook_permissions(self, 
                                 session: TableauSession, 
                                 workbook_id: str) -> PermissionsType:
        """
        Returns a list of permissions for a specific workbook. Available in all versions.
        
        Args:
            session: TableauSession object
            workbook_id: The ID of the workbook
            
        Returns:
            PermissionsType: The permissions for the workbook
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session), 
            ("workbook_id", workbook_id)
        )
        
        # Build URI and make request
        uri = self._api_client._build_uri(f"sites/{session.site_id}/workbooks/{workbook_id}/permissions")
        
        response_content = self._api_client._api_request(
            uri, "GET", 200, session=session
        )
        
        return self._api_client._get_response_as_object(response_content, PermissionsType)

    # Delete methods for users
    @ApiVersionAttribute(2, 0)
    def delete_data_source_permission_for_user(self, 
                                              session: TableauSession, 
                                              datasource_id: str, 
                                              user_id: str,
                                              capability_name: CapabilityTypeName, 
                                              capability_mode: CapabilityTypeMode) -> None:
        """
        Deletes a data source permission for a specific user. Available in all versions.
        
        Args:
            session: TableauSession object
            datasource_id: The ID of the datasource
            user_id: The ID of the user
            capability_name: The capability name
            capability_mode: The capability mode
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session), 
            ("datasource_id", datasource_id), 
            ("user_id", user_id)
        )
        
        # Build URI and make request
        uri = self._api_client._build_uri(
            f"sites/{session.site_id}/datasources/{datasource_id}/permissions/users/{user_id}/{capability_name.name}/{capability_mode.name}"
        )
        
        self._api_client._api_request(uri, "DELETE", 204, session=session)

    @ApiVersionAttribute(2, 0)
    def delete_data_source_permission_for_group(self, 
                                               session: TableauSession, 
                                               datasource_id: str, 
                                               group_id: str,
                                               capability_name: CapabilityTypeName, 
                                               capability_mode: CapabilityTypeMode) -> None:
        """
        Deletes a data source permission for a specific group. Available in all versions.
        
        Args:
            session: TableauSession object
            datasource_id: The ID of the datasource
            group_id: The ID of the group
            capability_name: The capability name
            capability_mode: The capability mode
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session), 
            ("datasource_id", datasource_id), 
            ("group_id", group_id)
        )
        
        # Build URI and make request
        uri = self._api_client._build_uri(
            f"sites/{session.site_id}/datasources/{datasource_id}/permissions/groups/{group_id}/{capability_name.name}/{capability_mode.name}"
        )
        
        self._api_client._api_request(uri, "DELETE", 204, session=session)

    @ApiVersionAttribute(2, 0)
    def delete_project_permission_for_user(self, 
                                          session: TableauSession, 
                                          project_id: str, 
                                          user_id: str,
                                          capability_name: CapabilityTypeName, 
                                          capability_mode: CapabilityTypeMode) -> None:
        """
        Deletes a project permission for a specific user. Available in all versions.
        
        Args:
            session: TableauSession object
            project_id: The ID of the project
            user_id: The ID of the user
            capability_name: The capability name
            capability_mode: The capability mode
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session), 
            ("project_id", project_id), 
            ("user_id", user_id)
        )
        
        # Build URI and make request
        uri = self._api_client._build_uri(
            f"sites/{session.site_id}/projects/{project_id}/permissions/users/{user_id}/{capability_name.name}/{capability_mode.name}"
        )
        
        self._api_client._api_request(uri, "DELETE", 204, session=session)

    @ApiVersionAttribute(2, 0)
    def delete_project_permission_for_group(self, 
                                           session: TableauSession, 
                                           project_id: str, 
                                           group_id: str,
                                           capability_name: CapabilityTypeName, 
                                           capability_mode: CapabilityTypeMode) -> None:
        """
        Deletes a project permission for a specific group. Available in all versions.
        
        Args:
            session: TableauSession object
            project_id: The ID of the project
            group_id: The ID of the group
            capability_name: The capability name
            capability_mode: The capability mode
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session), 
            ("project_id", project_id), 
            ("group_id", group_id)
        )
        
        # Build URI and make request
        uri = self._api_client._build_uri(
            f"sites/{session.site_id}/projects/{project_id}/permissions/groups/{group_id}/{capability_name.name}/{capability_mode.name}"
        )
        
        self._api_client._api_request(uri, "DELETE", 204, session=session)

    @ApiVersionAttribute(2, 0)
    def delete_default_data_source_permission_for_user(self, 
                                                      session: TableauSession, 
                                                      project_id: str, 
                                                      user_id: str,
                                                      capability_name: CapabilityTypeName, 
                                                      capability_mode: CapabilityTypeMode) -> None:
        """
        Deletes a default project data source permission for a specific user. Available in all versions.
        
        Args:
            session: TableauSession object
            project_id: The ID of the project
            user_id: The ID of the user
            capability_name: The capability name
            capability_mode: The capability mode
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session), 
            ("project_id", project_id), 
            ("user_id", user_id)
        )
        
        # Build URI and make request
        uri = self._api_client._build_uri(
            f"sites/{session.site_id}/projects/{project_id}/default-permissions/datasources/users/{user_id}/{capability_name.name}/{capability_mode.name}"
        )
        
        self._api_client._api_request(uri, "DELETE", 204, session=session)

    @ApiVersionAttribute(2, 0)
    def delete_default_data_source_permission_for_group(self, 
                                                       session: TableauSession, 
                                                       project_id: str, 
                                                       group_id: str,
                                                       capability_name: CapabilityTypeName, 
                                                       capability_mode: CapabilityTypeMode) -> None:
        """
        Deletes a default project data source permission for a specific group. Available in all versions.
        
        Args:
            session: TableauSession object
            project_id: The ID of the project
            group_id: The ID of the group
            capability_name: The capability name
            capability_mode: The capability mode
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session), 
            ("project_id", project_id), 
            ("group_id", group_id)
        )
        
        # Build URI and make request
        uri = self._api_client._build_uri(
            f"sites/{session.site_id}/projects/{project_id}/default-permissions/datasources/groups/{group_id}/{capability_name.name}/{capability_mode.name}"
        )
        
        self._api_client._api_request(uri, "DELETE", 204, session=session)

    @ApiVersionAttribute(2, 0)
    def delete_default_workbook_permission_for_user(self, 
                                                   session: TableauSession, 
                                                   project_id: str, 
                                                   user_id: str,
                                                   capability_name: CapabilityTypeName, 
                                                   capability_mode: CapabilityTypeMode) -> None:
        """
        Deletes a default project workbook permission for a specific user. Available in all versions.
        
        Args:
            session: TableauSession object
            project_id: The ID of the project
            user_id: The ID of the user
            capability_name: The capability name
            capability_mode: The capability mode
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session), 
            ("project_id", project_id), 
            ("user_id", user_id)
        )
        
        # Build URI and make request
        uri = self._api_client._build_uri(
            f"sites/{session.site_id}/projects/{project_id}/default-permissions/workbooks/users/{user_id}/{capability_name.name}/{capability_mode.name}"
        )
        
        self._api_client._api_request(uri, "DELETE", 204, session=session)

    @ApiVersionAttribute(2, 0)
    def delete_default_workbook_permission_for_group(self, 
                                                    session: TableauSession, 
                                                    project_id: str, 
                                                    group_id: str,
                                                    capability_name: CapabilityTypeName, 
                                                    capability_mode: CapabilityTypeMode) -> None:
        """
        Deletes a default project workbook permission for a specific group. Available in all versions.
        
        Args:
            session: TableauSession object
            project_id: The ID of the project
            group_id: The ID of the group
            capability_name: The capability name
            capability_mode: The capability mode
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session), 
            ("project_id", project_id), 
            ("group_id", group_id)
        )
        
        # Build URI and make request
        uri = self._api_client._build_uri(
            f"sites/{session.site_id}/projects/{project_id}/default-permissions/workbooks/groups/{group_id}/{capability_name.name}/{capability_mode.name}"
        )
        
        self._api_client._api_request(uri, "DELETE", 204, session=session)

    @ApiVersionAttribute(2, 0)
    def delete_workbook_permission_for_user(self, 
                                        session: TableauSession, 
                                        workbook_id: str, 
                                        user_id: str,
                                        capability_name: CapabilityTypeName, 
                                        capability_mode: CapabilityTypeMode) -> None:
        """
        Deletes a workbook permission for a specific user. Available in all versions.
        
        Args:
            session: TableauSession object
            workbook_id: The ID of the workbook
            user_id: The ID of the user
            capability_name: The capability name
            capability_mode: The capability mode
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session), 
            ("workbook_id", workbook_id), 
            ("user_id", user_id)
        )
        
        # Build URI and make request
        uri = self._api_client._build_uri(
            f"sites/{session.site_id}/workbooks/{workbook_id}/permissions/users/{user_id}/{capability_name.name}/{capability_mode.name}"
        )
        
        self._api_client._api_request(uri, "DELETE", 204, session=session)

    @ApiVersionAttribute(2, 0)
    def delete_workbook_permission_for_group(self, 
                                            session: TableauSession, 
                                            workbook_id: str, 
                                            group_id: str,
                                            capability_name: CapabilityTypeName, 
                                            capability_mode: CapabilityTypeMode) -> None:
        """
        Deletes a workbook permission for a specific group. Available in all versions.
        
        Args:
            session: TableauSession object
            workbook_id: The ID of the workbook
            group_id: The ID of the group
            capability_name: The capability name
            capability_mode: The capability mode
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session), 
            ("workbook_id", workbook_id), 
            ("group_id", group_id)
        )
        
        # Build URI and make request
        uri = self._api_client._build_uri(
            f"sites/{session.site_id}/workbooks/{workbook_id}/permissions/groups/{group_id}/{capability_name.name}/{capability_mode.name}"
        )
        
        self._api_client._api_request(uri, "DELETE", 204, session=session)

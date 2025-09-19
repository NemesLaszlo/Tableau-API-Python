from typing import Optional, List, Tuple, TYPE_CHECKING
from tableau_api_client.models.ts_api import (
    GroupType, UserType, PaginationType, JobType, GroupListType, UserListType,
    ImportDirectiveType, ImportSourceType, SiteRoleType, SiteUserAuthSettingType
)
from tableau_api_client.models.tableau_session import TableauSession
from tableau_api_client.attributes.api_version_attribute import ApiVersionAttribute
from tableau_api_client.attributes.on_premise_only_attribute import OnPremiseOnlyAttribute

if TYPE_CHECKING:
    from tableau_api_client.tableau_api_client import TableauApiClient


class TableauUsersGroupsClient:
    """Client for Tableau Server groups and users operations"""
    
    def __init__(self, api_client: "TableauApiClient"):
        """
        Initialize groups and users client
        
        Args:
            api_client: Instance of TableauApiClient
        """
        self._api_client = api_client
    
    @ApiVersionAttribute(2, 0)
    def create_group(self, session: TableauSession, group_name: str) -> GroupType:
        """
        Creates a group in Tableau Server. Available in all versions
        
        Args:
            session: TableauSession object
            group_name: The name for the new group
            
        Returns:
            GroupType: The created group object
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session), 
            ("group_name", group_name)
        )
        
        # Create group object
        group = GroupType()
        group.name = group_name
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/groups")
        request_body = self._api_client.get_object_as_request_content(group)
        
        response_content = self._api_client.api_request(
            uri, "POST", 201, session=session, body=request_body
        )
        
        # Parse response
        return self._api_client.get_response_as_object(response_content, GroupType)
    
    @ApiVersionAttribute(2, 0)
    def create_group_from_active_directory(
        self, 
        session: TableauSession, 
        ad_group_name: str, 
        domain_name: str, 
        site_role: SiteRoleType
    ) -> GroupType:
        """
        Creates a group in Tableau Server from Active Directory. 
        This runs synchronously and may time out if the AD group contains a lot of users.
        
        Args:
            session: TableauSession object
            ad_group_name: The name of the Active Directory group to import
            domain_name: The domain of the Active Directory group to import
            site_role: The site role to assign to users imported from Active Directory
            
        Returns:
            GroupType: The created group object
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("ad_group_name", ad_group_name),
            ("domain_name", domain_name)
        )
        
        # Create import directive
        import_directive = ImportDirectiveType()
        import_directive.source = ImportSourceType.ACTIVE_DIRECTORY
        import_directive.domain_name = domain_name
        import_directive.site_role = site_role
        
        # Create group object
        group = GroupType()
        group.name = ad_group_name
        group.import_value = import_directive
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/groups")
        request_body = self._api_client.get_object_as_request_content(group)
        
        response_content = self._api_client.api_request(
            uri, "POST", 201, session=session, body=request_body
        )
        
        # Parse response
        return self._api_client.get_response_as_object(response_content, GroupType)
    
    @ApiVersionAttribute(2, 0)
    def create_group_from_active_directory_as_job(
        self,
        session: TableauSession,
        ad_group_name: str,
        domain_name: str,
        site_role: SiteRoleType
    ) -> JobType:
        """
        Creates a group in Tableau Server from Active Directory as a background job.
        
        Args:
            session: TableauSession object
            ad_group_name: The name of the Active Directory group to import
            domain_name: The domain of the Active Directory group to import
            site_role: The site role to assign to users imported from Active Directory
            
        Returns:
            JobType: The background job object
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("ad_group_name", ad_group_name),
            ("domain_name", domain_name)
        )
        
        # Create import directive
        import_directive = ImportDirectiveType()
        import_directive.source = ImportSourceType.ACTIVE_DIRECTORY
        import_directive.domain_name = domain_name
        import_directive.site_role = site_role
        
        # Create group object
        group = GroupType()
        group.name = ad_group_name
        group.import_value = import_directive
        
        # Build URI and make request (with asJob parameter)
        uri = self._api_client.build_uri(f"sites/{session.site_id}/groups", ("asJob", "true"))
        request_body = self._api_client.get_object_as_request_content(group)
        
        response_content = self._api_client.api_request(
            uri, "POST", 202, session=session, body=request_body
        )
        
        # Parse response
        return self._api_client.get_response_as_object(response_content, JobType)
    
    @ApiVersionAttribute(2, 0)
    def update_group(self, session: TableauSession, group_id: str, new_group_name: str) -> GroupType:
        """
        Updates a group in Tableau Server.
        
        Args:
            session: TableauSession object
            group_id: The ID of the group to update
            new_group_name: The new name for the group
            
        Returns:
            GroupType: The updated group object
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("group_id", group_id),
            ("new_group_name", new_group_name)
        )
        
        # Create group object
        group = GroupType()
        group.name = new_group_name
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/groups/{group_id}")
        request_body = self._api_client.get_object_as_request_content(group)
        
        response_content = self._api_client.api_request(
            uri, "PUT", 200, session=session, body=request_body
        )
        
        # Parse response
        return self._api_client.get_response_as_object(response_content, GroupType)
    
    @ApiVersionAttribute(2, 0)
    def add_user_to_group(self, session: TableauSession, group_id: str, user_id: str) -> UserType:
        """
        Adds a user to a group in Tableau Server.
        
        Args:
            session: TableauSession object
            group_id: The ID of the group to add the user to
            user_id: The ID of the user to add
            
        Returns:
            UserType: The user object
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("group_id", group_id),
            ("user_id", user_id)
        )
        
        # Create user object
        user = UserType()
        user.id = user_id
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/groups/{group_id}/users")
        request_body = self._api_client.get_object_as_request_content(user)
        
        response_content = self._api_client.api_request(
            uri, "POST", 200, session=session, body=request_body
        )
        
        # Parse response
        return self._api_client.get_response_as_object(response_content, UserType)
    
    @ApiVersionAttribute(2, 0)
    def add_user_to_site(
        self, 
        session: TableauSession, 
        user_name: str, 
        site_role: SiteRoleType, 
        auth_setting: Optional[SiteUserAuthSettingType] = None
    ) -> UserType:
        """
        Adds a user to the current site.
        
        Args:
            session: TableauSession object
            user_name: The name of the user
            site_role: The site role for the user
            auth_setting: Authentication setting (optional)
            
        Returns:
            UserType: The created user object
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("user_name", user_name)
        )
        
        # Create user object
        user = UserType()
        user.name = user_name
        user.site_role = site_role
        if auth_setting is not None:
            user.auth_setting = auth_setting
        else:
            user.auth_setting = SiteUserAuthSettingType.SERVER_DEFAULT
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/users")
        request_body = self._api_client.get_object_as_request_content(user)
        
        response_content = self._api_client.api_request(
            uri, "POST", 201, session=session, body=request_body
        )
        
        # Parse response
        return self._api_client.get_response_as_object(response_content, UserType)
    
    @ApiVersionAttribute(3, 7)
    def get_groups_of_user(
        self, 
        session: TableauSession, 
        user_id: str, 
        page_size: int = 100, 
        page_number: int = 1
    ) -> Tuple[PaginationType, List[GroupType]]:
        """
        Gets the groups of a user. Can only be called by site/server administrators.
        
        Args:
            session: TableauSession object
            user_id: The ID of the user to get the groups for
            page_size: The number of items to return (1-1000, default: 100)
            page_number: The page number for paging (default: 1)
            
        Returns:
            Tuple containing pagination info and list of groups
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("user_id", user_id)
        )
        self._api_client.check_parameters_between(
            ("page_size", page_size, 1, 1000),
            ("page_number", page_number, 1, 2147483647)
        )
        
        # Build URI and make request
        uri = self._api_client.build_uri(
            f"sites/{session.site_id}/users/{user_id}/groups",
            ("pageSize", str(page_size)),
            ("pageNumber", str(page_number))
        )
        
        response_content = self._api_client.api_request(
            uri, "GET", 200, session=session
        )
        
        # Parse response
        pagination, group_list = self._api_client.get_response_as_objects(
            response_content, PaginationType, GroupListType
        )
        
        groups = group_list.group if group_list and group_list.group else []
        return pagination, groups
    
    @ApiVersionAttribute(2, 0)
    def get_users_in_group(
        self, 
        session: TableauSession, 
        group_id: str, 
        page_size: int = 100, 
        page_number: int = 1
    ) -> Tuple[PaginationType, List[UserType]]:
        """
        Gets users in a group.
        
        Args:
            session: TableauSession object
            group_id: The ID of the group to get the users for
            page_size: The number of items to return (1-1000, default: 100)
            page_number: The page number for paging (default: 1)
            
        Returns:
            Tuple containing pagination info and list of users
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("group_id", group_id)
        )
        self._api_client.check_parameters_between(
            ("page_size", page_size, 1, 1000),
            ("page_number", page_number, 1, 2147483647)
        )
        
        # Build URI and make request
        uri = self._api_client.build_uri(
            f"sites/{session.site_id}/groups/{group_id}/users",
            ("pageSize", str(page_size)),
            ("pageNumber", str(page_number))
        )
        
        response_content = self._api_client.api_request(
            uri, "GET", 200, session=session
        )
        
        # Parse response
        pagination, user_list = self._api_client.get_response_as_objects(
            response_content, PaginationType, UserListType
        )
        
        users = user_list.user if user_list and user_list.user else []
        return pagination, users
    
    @ApiVersionAttribute(2, 0)
    def get_users_on_site(
        self,
        session: TableauSession,
        page_size: int = 100,
        page_number: int = 1,
        fields_expression: Optional[str] = None,
        filter_expression: Optional[str] = None,
        sort_expression: Optional[str] = None
    ) -> Tuple[PaginationType, List[UserType]]:
        """
        Gets users on current site.
        
        Args:
            session: TableauSession object
            page_size: The number of items to return (1-1000, default: 100)
            page_number: The page number for paging (default: 1)
            fields_expression: Expression to specify fields to return (optional)
            filter_expression: Expression to filter users (optional)
            sort_expression: Expression to specify sort order (optional)
            
        Returns:
            Tuple containing pagination info and list of users
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(("session", session))
        self._api_client.check_parameters_between(
            ("page_size", page_size, 1, 1000),
            ("page_number", page_number, 1, 2147483647)
        )
        
        # Build query parameters
        params = [
            ("pageSize", str(page_size)),
            ("pageNumber", str(page_number))
        ]
        
        if fields_expression:
            params.append(("fields", fields_expression))
        if filter_expression:
            params.append(("filter", filter_expression))
        if sort_expression:
            params.append(("sort", sort_expression))
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/users", *params)
        
        response_content = self._api_client.api_request(
            uri, "GET", 200, session=session
        )
        
        # Parse response
        pagination, user_list = self._api_client.get_response_as_objects(
            response_content, PaginationType, UserListType
        )
        
        users = user_list.user if user_list and user_list.user else []
        return pagination, users
    
    @ApiVersionAttribute(2, 0)
    def query_groups(
        self,
        session: TableauSession,
        page_size: int = 100,
        page_number: int = 1,
        filter_expression: Optional[str] = None,
        sort_expression: Optional[str] = None
    ) -> Tuple[PaginationType, List[GroupType]]:
        """
        Queries groups on current site.
        
        Args:
            session: TableauSession object
            page_size: The number of items to return (1-1000, default: 100)
            page_number: The page number for paging (default: 1)
            filter_expression: Expression to filter groups (optional)
            sort_expression: Expression to specify sort order (optional)
            
        Returns:
            Tuple containing pagination info and list of groups
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(("session", session))
        self._api_client.check_parameters_between(
            ("page_size", page_size, 1, 1000),
            ("page_number", page_number, 1, 2147483647)
        )
        
        # Build query parameters
        params = [
            ("pageSize", str(page_size)),
            ("pageNumber", str(page_number))
        ]
        
        if filter_expression:
            params.append(("filter", filter_expression))
        if sort_expression:
            params.append(("sort", sort_expression))
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/groups", *params)
        
        response_content = self._api_client.api_request(
            uri, "GET", 200, session=session
        )
        
        # Parse response
        pagination, group_list = self._api_client.get_response_as_objects(
            response_content, PaginationType, GroupListType
        )
        
        groups = group_list.group if group_list and group_list.group else []
        return pagination, groups
    
    @ApiVersionAttribute(2, 0)
    def query_user(self, session: TableauSession, user_id: str) -> UserType:
        """
        Queries a user on the current site.
        
        Args:
            session: TableauSession object
            user_id: The ID of the user to get information for
            
        Returns:
            UserType: The user object
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("user_id", user_id)
        )
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/users/{user_id}")
        
        response_content = self._api_client.api_request(
            uri, "GET", 200, session=session
        )
        
        # Parse response
        return self._api_client.get_response_as_object(response_content, UserType)
    
    @ApiVersionAttribute(2, 0)
    def update_user(
        self,
        session: TableauSession,
        user_id: str,
        new_full_name: Optional[str] = None,
        new_email: Optional[str] = None,
        new_password: Optional[str] = None,
        new_site_role: Optional[SiteRoleType] = None,
        new_auth_setting: Optional[SiteUserAuthSettingType] = None,
        new_display_name: Optional[str] = None
    ) -> UserType:
        """
        Updates a user on the current site.
        
        Args:
            session: TableauSession object
            user_id: The ID of the user to update
            new_full_name: The new full name for the user (optional)
            new_email: The new email address for the user (optional)
            new_password: The new password for the user (optional)
            new_site_role: The new site role (optional)
            new_auth_setting: The new authentication setting (optional)
            new_display_name: The new display name (optional, experimental)
            
        Returns:
            UserType: The updated user object
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("user_id", user_id)
        )
        
        # Create user object with updates
        user = UserType()
        if new_display_name is not None:
            user.name = new_display_name
        if new_full_name is not None:
            user.full_name = new_full_name
        if new_email is not None:
            user.email = new_email
        if new_password is not None:
            user.password = new_password
        if new_site_role is not None:
            user.site_role = new_site_role
        if new_auth_setting is not None:
            user.auth_setting = new_auth_setting
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/users/{user_id}")
        request_body = self._api_client.get_object_as_request_content(user)
        
        response_content = self._api_client.api_request(
            uri, "PUT", 200, session=session, body=request_body
        )
        
        # Parse response
        return self._api_client.get_response_as_object(response_content, UserType)
    
    @ApiVersionAttribute(2, 0)
    def remove_user_from_group(self, session: TableauSession, group_id: str, user_id: str) -> None:
        """
        Removes a user from a group.
        
        Args:
            session: TableauSession object
            group_id: The ID of the group to remove the user from
            user_id: The ID of the user to remove
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("group_id", group_id),
            ("user_id", user_id)
        )
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/groups/{group_id}/users/{user_id}")
        
        self._api_client.api_request(
            uri, "DELETE", 204, session=session
        )
    
    @ApiVersionAttribute(2, 0)
    def remove_user_from_site(self, session: TableauSession, user_id: str) -> None:
        """
        Removes a user from a site.
        
        Args:
            session: TableauSession object
            user_id: The ID of the user to remove
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("user_id", user_id)
        )
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/users/{user_id}")
        
        self._api_client.api_request(
            uri, "DELETE", 204, session=session
        )
    
    @ApiVersionAttribute(2, 0)
    @OnPremiseOnlyAttribute()
    def delete_group(self, session: TableauSession, group_id: str) -> None:
        """
        Deletes a group. Not available in Tableau Online.
        
        Args:
            session: TableauSession object
            group_id: The ID of the group to delete
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            TableauOnlineNotSupportedException: If used with Tableau Online
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("group_id", group_id)
        )
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/groups/{group_id}")
        
        self._api_client.api_request(
            uri, "DELETE", 204, session=session
        )
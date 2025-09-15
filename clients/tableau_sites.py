from typing import Optional, List, Tuple, TYPE_CHECKING
from models.ts_api import (
    SiteType, SiteListType, SiteTypeAdminMode, PaginationType, 
    ViewListType, ViewType, ServerInfo
)
from models.tableau_session import TableauSession
from attributes.api_version_attribute import ApiVersionAttribute
from attributes.on_premise_only_attribute import OnPremiseOnlyAttribute

if TYPE_CHECKING:
    from tableau_api_client import TableauApiClient


class TableauSitesClient:
    """Client for Tableau Server sites operations"""
    
    def __init__(self, api_client: "TableauApiClient"):
        """
        Initialize sites client
        
        Args:
            api_client: Instance of TableauApiClient
        """
        self._api_client = api_client
    
    @ApiVersionAttribute(2, 4)
    def server_info(self) -> ServerInfo:
        """
        Returns the version of Tableau Server and the supported version of the REST API. 
        Versions 2.4+
        
        Returns:
            ServerInfo: Object containing server information including version details
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
        """
        self._api_client.check_endpoint_availability()
        
        # Build URI and make request
        uri = self._api_client.build_uri("serverinfo")
        
        response_content = self._api_client.api_request(
            uri, "GET", 200, session=None, body=None
        )
        
        # Parse and return response
        return self._api_client.get_response_as_object(response_content, ServerInfo)

    @ApiVersionAttribute(1, 0)
    @OnPremiseOnlyAttribute()
    def create_site(self, 
                    session: TableauSession,
                    name: str,
                    content_url: str,
                    admin_mode: Optional[SiteTypeAdminMode] = None,
                    tier_creator_capacity: Optional[int] = None,
                    tier_explorer_capacity: Optional[int] = None,
                    tier_viewer_capacity: Optional[int] = None,
                    storage_quota_mb: Optional[int] = None,
                    disable_subscriptions: Optional[bool] = False) -> SiteType:
        """
        Creates a site. Not available in Tableau Online. Versions 1.0+
        
        Args:
            session: The session to use
            name: The name of the site
            content_url: The site URL. This value can contain only characters that are valid in a URL
            admin_mode: Specify ContentAndUsers to allow site administrators to use the server interface 
                       and tabcmd commands to add and remove users. Specify ContentOnly to prevent 
                       site administrators from adding or removing users
            tier_creator_capacity: The maximum number of Creator users for the site
            tier_explorer_capacity: The maximum number of Explorer users for the site
            tier_viewer_capacity: The maximum number of Viewer users for the site
            storage_quota_mb: The maximum amount of space for the new site, in megabytes
            disable_subscriptions: Specify True to prevent users from being able to subscribe to workbooks
            
        Returns:
            SiteType: The created site object
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            TableauOnlineNotSupportedException: If used with Tableau Online
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("name", name),
            ("content_url", content_url)
        )
        
        # Create site object
        site = SiteType()
        site.name = name
        site.content_url = content_url
        site.admin_mode = admin_mode or SiteTypeAdminMode.CONTENT_ONLY
        
        if tier_creator_capacity is not None:
            site.tier_creator_capacity = tier_creator_capacity
        if tier_explorer_capacity is not None:
            site.tier_explorer_capacity = tier_explorer_capacity
        if tier_viewer_capacity is not None:
            site.tier_viewer_capacity = tier_viewer_capacity
        if storage_quota_mb is not None:
            site.storage_quota = str(storage_quota_mb)
        if disable_subscriptions is not None:
            site.disable_subscriptions = disable_subscriptions
        
        # Build URI and make request
        uri = self._api_client.build_uri("sites")
        request_body = self._api_client.get_object_as_request_content(site)
        
        response_content = self._api_client.api_request(
            uri, "POST", 201, session=session, body=request_body
        )
        
        # Parse response
        return self._api_client.get_response_as_object(response_content, SiteType)

    @ApiVersionAttribute(1, 0)
    def query_site_by_id(self, session: TableauSession, site_id: str) -> SiteType:
        """
        Queries a single Site by siteId. Available in all versions
        
        Args:
            session: The session to use
            site_id: The id of the site to query
            
        Returns:
            SiteType: The site object
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("site_id", site_id)
        )
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{site_id}")
        
        response_content = self._api_client.api_request(
            uri, "GET", 200, session=session, body=None
        )
        
        # Parse response
        return self._api_client.get_response_as_object(response_content, SiteType)

    @ApiVersionAttribute(1, 0)
    def query_site_by_name(self, session: TableauSession, site_name: str) -> SiteType:
        """
        Queries a single Site by siteName. Available in all versions
        
        Args:
            session: The session to use
            site_name: The name of the site to query
            
        Returns:
            SiteType: The site object
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("site_name", site_name)
        )
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{site_name}", ("key", "name"))
        
        response_content = self._api_client.api_request(
            uri, "GET", 200, session=session, body=None
        )
        
        # Parse response
        return self._api_client.get_response_as_object(response_content, SiteType)

    @ApiVersionAttribute(1, 0)
    def query_site_by_content_url(self, session: TableauSession, site_content_url: str) -> SiteType:
        """
        Queries a single Site by siteContentUrl. Available in all versions
        
        Args:
            session: The session to use
            site_content_url: The content URL of the site to query
            
        Returns:
            SiteType: The site object
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(("session", session))
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{site_content_url}", ("key", "contentUrl"))
        
        response_content = self._api_client.api_request(
            uri, "GET", 200, session=session, body=None
        )
        
        # Parse response
        return self._api_client.get_response_as_object(response_content, SiteType)

    @ApiVersionAttribute(1, 0)
    @OnPremiseOnlyAttribute()
    def query_sites(self, 
                    session: TableauSession,
                    page_size: int = 100,
                    page_number: int = 1) -> Tuple[PaginationType, List[SiteType]]:
        """
        Queries sites. Not available in Tableau Online. Available in all versions
        
        Args:
            session: The session to use
            page_size: The number of items to return in one response. The minimum is 1. The maximum is 1000
            page_number: The offset for paging. The default is 1
            
        Returns:
            Tuple containing pagination info and list of sites
            
        Raises:
            TableauRequestException: If the request fails
            TableauOnlineNotSupportedException: If used with Tableau Online
            ValueError: If required parameters are missing or out of range
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(("session", session))
        self._api_client.check_parameters_between(
            ("page_size", page_size, 1, 1000),
            ("page_number", page_number, 1, 2147483647)
        )
        
        # Build URI and make request
        uri = self._api_client.build_uri("sites", 
                                        ("pageSize", page_size),
                                        ("pageNumber", page_number))
        
        response_content = self._api_client.api_request(
            uri, "GET", 200, session=session, body=None
        )
        
        # Parse response
        pagination, site_list = self._api_client.get_response_as_objects(
            response_content, PaginationType, SiteListType
        )
        
        sites = site_list.site if site_list and site_list.site else []
        return pagination, sites

    @ApiVersionAttribute(2, 2)
    def query_views_for_site(self,
                             session: TableauSession,
                             page_size: int = 100,
                             page_number: int = 1,
                             include_usage_statistics: Optional[bool] = None,
                             fields_expression: Optional[str] = None,
                             filter_expression: Optional[str] = None,
                             sort_expression: Optional[str] = None) -> Tuple[PaginationType, List[ViewType]]:
        """
        Queries views for specified site. Versions 2.2+
        
        Args:
            session: The session to use
            page_size: The number of items to return in one response. The minimum is 1. The maximum is 1000
            page_number: The offset for paging. The default is 1
            include_usage_statistics: True to return usage statistics. The default is False
            fields_expression: An expression that lets you specify the set of available fields to return
            filter_expression: Filter expression for the views
            sort_expression: Sort expression for the views
            
        Returns:
            Tuple containing pagination info and list of views
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing or out of range
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(("session", session))
        self._api_client.check_parameters_between(
            ("page_size", page_size, 1, 1000),
            ("page_number", page_number, 1, 2147483647)
        )
        
        # Build URL parameters
        url_params = [
            ("pageSize", page_size),
            ("pageNumber", page_number)
        ]
        
        if include_usage_statistics is not None:
            url_params.append(("includeUsageStatistics", str(include_usage_statistics).lower()))
        if fields_expression:
            url_params.append(("fields", fields_expression))
        if filter_expression:
            url_params.append(("filter", filter_expression))
        if sort_expression:
            url_params.append(("sort", sort_expression))
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/views", *url_params)
        
        response_content = self._api_client.api_request(
            uri, "GET", 200, session=session, body=None
        )
        
        # Parse response
        pagination, view_list = self._api_client.get_response_as_objects(
            response_content, PaginationType, ViewListType
        )
        
        views = view_list.view if view_list and view_list.view else []
        return pagination, views

    @ApiVersionAttribute(1, 0)
    @OnPremiseOnlyAttribute()
    def update_site(self,
                    session: TableauSession,
                    site_id: str,
                    new_name: Optional[str] = None,
                    new_content_url: Optional[str] = None,
                    new_admin_mode: Optional[SiteTypeAdminMode] = None,
                    new_tier_creator_capacity: Optional[int] = None,
                    new_tier_explorer_capacity: Optional[int] = None,
                    new_tier_viewer_capacity: Optional[int] = None,
                    new_storage_quota: Optional[int] = None,
                    new_disable_subscriptions: Optional[bool] = None,
                    new_revision_history_enabled: Optional[bool] = None,
                    new_revision_limit: Optional[int] = None) -> SiteType:
        """
        Updates a site. Not available in Tableau Online. Available in all versions
        
        Args:
            session: The session to use
            site_id: The id of the site to update
            new_name: The new name of the site
            new_content_url: The new site URL
            new_admin_mode: The new admin mode for the site
            new_tier_creator_capacity: The new maximum number of Creator users
            new_tier_explorer_capacity: The new maximum number of Explorer users
            new_tier_viewer_capacity: The new maximum number of Viewer users
            new_storage_quota: The new maximum amount of space for the site, in megabytes
            new_disable_subscriptions: True to prevent users from subscribing to workbooks
            new_revision_history_enabled: True if the site maintains revisions for changes
            new_revision_limit: An integer between 2 and 10000 to indicate a limited number of revisions
            
        Returns:
            SiteType: The updated site object
            
        Raises:
            TableauRequestException: If the request fails
            TableauOnlineNotSupportedException: If used with Tableau Online
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("site_id", site_id)
        )
        
        # Create site object with updates
        site = SiteType()
        if new_name is not None:
            site.name = new_name
        if new_content_url is not None:
            site.content_url = new_content_url
        if new_admin_mode is not None:
            site.admin_mode = new_admin_mode
        if new_tier_creator_capacity is not None:
            site.tier_creator_capacity = new_tier_creator_capacity
        if new_tier_explorer_capacity is not None:
            site.tier_explorer_capacity = new_tier_explorer_capacity
        if new_tier_viewer_capacity is not None:
            site.tier_viewer_capacity = new_tier_viewer_capacity
        if new_storage_quota is not None:
            site.storage_quota = str(new_storage_quota)
        if new_disable_subscriptions is not None:
            site.disable_subscriptions = new_disable_subscriptions
        if new_revision_history_enabled is not None:
            site.revision_history_enabled = new_revision_history_enabled
        if new_revision_limit is not None:
            site.revision_limit = new_revision_limit
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{site_id}")
        request_body = self._api_client.get_object_as_request_content(site)
        
        response_content = self._api_client.api_request(
            uri, "PUT", 200, session=session, body=request_body
        )
        
        # Parse response
        return self._api_client.get_response_as_object(response_content, SiteType)

    @ApiVersionAttribute(1, 0)
    @OnPremiseOnlyAttribute()
    def delete_site_by_id(self, session: TableauSession, site_id: str) -> None:
        """
        Deletes a single Site by siteId. Not available in Tableau Online. Available in all versions
        
        Args:
            session: The session to use
            site_id: The id of the site to delete
            
        Raises:
            TableauRequestException: If the request fails
            TableauOnlineNotSupportedException: If used with Tableau Online
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("site_id", site_id)
        )
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{site_id}")
        
        self._api_client.api_request(
            uri, "DELETE", 204, session=session, body=None
        )

    @ApiVersionAttribute(1, 0)
    @OnPremiseOnlyAttribute()
    def delete_site_by_name(self, session: TableauSession, site_name: str) -> None:
        """
        Deletes a single Site by siteName. Not available in Tableau Online. Available in all versions
        
        Args:
            session: The session to use
            site_name: The name of the site to delete
            
        Raises:
            TableauRequestException: If the request fails
            TableauOnlineNotSupportedException: If used with Tableau Online
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("site_name", site_name)
        )
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{site_name}", ("key", "name"))
        
        self._api_client.api_request(
            uri, "DELETE", 204, session=session, body=None
        )

    @ApiVersionAttribute(1, 0)
    @OnPremiseOnlyAttribute()
    def delete_site_by_content_url(self, session: TableauSession, site_content_url: str) -> None:
        """
        Deletes a single Site by siteContentUrl. Not available in Tableau Online. Available in all versions
        
        Args:
            session: The session to use
            site_content_url: The content URL of the site to delete
            
        Raises:
            TableauRequestException: If the request fails
            TableauOnlineNotSupportedException: If used with Tableau Online
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("site_content_url", site_content_url)
        )
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{site_content_url}", ("key", "contentUrl"))
        
        self._api_client.api_request(
            uri, "DELETE", 204, session=session, body=None
        )
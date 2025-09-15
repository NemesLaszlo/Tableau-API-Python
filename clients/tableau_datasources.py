from typing import Optional, List, Tuple, TYPE_CHECKING
from io import BytesIO
from enums.datasource_file_type import DatasourceFileType
from models.ts_api import (
    TagListType, TagType, DataSourceType, DataSourceListType, 
    PaginationType, ConnectionListType, ConnectionType, JobType, 
    ProjectType, UserType
)
from models.tableau_session import TableauSession
from attributes.api_version_attribute import ApiVersionAttribute

if TYPE_CHECKING:
    from tableau_api_client import TableauApiClient

class TableauDatasourcesClient:
    """Client for Tableau Server datasources operations"""
    
    def __init__(self, api_client: "TableauApiClient"):
        """
        Initialize datasources client
        
        Args:
            api_client: Instance of TableauApiClient
        """
        self._api_client = api_client
    
    @ApiVersionAttribute(1, 0)
    def add_tags_to_data_source(self, 
                                session: TableauSession, 
                                datasource_id: str, 
                                tags_to_add: List[str]) -> List[TagType]:
        """
        Adds one or more tags to the specified data source. Available in all versions.
        
        Args:
            session: TableauSession object
            datasource_id: The ID of the data source to add tags to
            tags_to_add: List of tag names to add to the data source
            
        Returns:
            List[TagType]: List of tags that were added
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing or invalid
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("datasource_id", datasource_id),
            ("tags_to_add", tags_to_add)
        )
        self._api_client.check_empty_arrays(("tags_to_add", tags_to_add))
        
        # Create tag list object
        tag_list = TagListType()
        tag_list.tag = [TagType(label=tag) for tag in tags_to_add]
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/datasources/{datasource_id}/tags")
        request_body = self._api_client.get_object_as_request_content(tag_list)
        
        response_content = self._api_client.api_request(
            uri, "PUT", 200, session=session, body=request_body
        )
        
        # Parse response
        response_tags = self._api_client.get_response_as_object(response_content, TagListType)
        return response_tags.tag if response_tags and response_tags.tag else []
    
    @ApiVersionAttribute(1, 0)
    def delete_tag_from_data_source(self, 
                                   session: TableauSession, 
                                   datasource_id: str, 
                                   tag: str) -> None:
        """
        Deletes a tag from the specified data source. Available in all versions.
        
        Args:
            session: TableauSession object
            datasource_id: The ID of the data source to remove the tag from
            tag: The name of the tag to remove from the data source
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("datasource_id", datasource_id),
            ("tag", tag)
        )
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/datasources/{datasource_id}/tags/{tag}")
        
        self._api_client.api_request(
            uri, "DELETE", 204, session=session, body=None
        )
    
    @ApiVersionAttribute(1, 0)
    def query_data_source(self, 
                         session: TableauSession, 
                         datasource_id: str) -> DataSourceType:
        """
        Returns information about the specified data source. Available in all versions.
        
        Args:
            session: TableauSession object
            datasource_id: The ID of the data source to get
            
        Returns:
            DataSourceType: Information about the data source
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("datasource_id", datasource_id)
        )
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/datasources/{datasource_id}")
        
        response_content = self._api_client.api_request(
            uri, "GET", 200, session=session, body=None
        )
        
        # Parse response
        return self._api_client.get_response_as_object(response_content, DataSourceType)
    
    @ApiVersionAttribute(1, 0)
    def query_data_sources(self, 
                          session: TableauSession,
                          page_size: int = 100,
                          page_number: int = 1,
                          fields_expression: Optional[str] = None,
                          filter_expression: Optional[str] = None,
                          sort_expression: Optional[str] = None) -> Tuple[PaginationType, List[DataSourceType]]:
        """
        Returns a list of data sources on the specified site, with optional parameters for specifying 
        the paging of large results. Available in all versions.
        
        Args:
            session: TableauSession object
            page_size: The number of items to return in one response (1-1000)
            page_number: The offset for paging (default: 1)
            fields_expression: Expression to specify subset of data sources to return
            filter_expression: Expression to filter results
            sort_expression: Expression to specify sort order
            
        Returns:
            Tuple[PaginationType, List[DataSourceType]]: Pagination info and list of data sources
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing or invalid
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(("session", session))
        self._api_client.check_parameters_between(
            ("page_size", page_size, 1, 1000),
            ("page_number", page_number, 1, 2147483647)
        )
        
        # Build query parameters
        query_params = [
            ("pageSize", str(page_size)),
            ("pageNumber", str(page_number))
        ]
        
        if fields_expression:
            query_params.append(("fields", fields_expression))
        if filter_expression:
            query_params.append(("filter", filter_expression))
        if sort_expression:
            query_params.append(("sort", sort_expression))
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/datasources", *query_params)
        
        response_content = self._api_client.api_request(
            uri, "GET", 200, session=session, body=None
        )
        
        # Parse response - expecting both pagination and datasource list
        pagination, datasource_list = self._api_client.get_response_as_objects(
            response_content, PaginationType, DataSourceListType
        )
        
        datasources = datasource_list.datasource if datasource_list and datasource_list.datasource else []
        return pagination, datasources
    
    @ApiVersionAttribute(2, 3)
    def query_data_source_connections(self, 
                                    session: TableauSession, 
                                    datasource_id: str) -> List[ConnectionType]:
        """
        Returns a list of data connections for the specified data source. Versions 2.3+
        
        Args:
            session: TableauSession object
            datasource_id: The ID of the data source to return connection information about
            
        Returns:
            List[ConnectionType]: List of data connections
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("datasource_id", datasource_id)
        )
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/datasources/{datasource_id}/connections")
        
        response_content = self._api_client.api_request(
            uri, "GET", 200, session=session, body=None
        )
        
        # Parse response
        connection_list = self._api_client.get_response_as_object(response_content, ConnectionListType)
        return connection_list.connection if connection_list and connection_list.connection else []
    
    @ApiVersionAttribute(2, 0)
    def download_data_source(self, 
                           session: TableauSession, 
                           datasource_id: str,
                           include_extract: bool = True) -> Tuple[BytesIO, DatasourceFileType]:
        """
        Downloads a data source in .tdsx format. Returns the stream and file type. Available in versions 2.0+
        
        Args:
            session: TableauSession object
            datasource_id: The ID of the data source to download
            include_extract: Whether to include extract in download (default: True)
            
        Returns:
            Tuple[BytesIO, DatasourceFileType]: Stream containing the file data and file type
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("datasource_id", datasource_id)
        )
        
        # Build URI and make request
        uri = self._api_client.build_uri(
            f"sites/{session.site_id}/datasources/{datasource_id}/content",
            ("includeExtract", str(include_extract).lower())
        )
        
        with self._api_client.build_client(session.token) as http_session:
            response = http_session.get(uri, timeout=self._api_client.timeout.total_seconds())
            
            if response.status_code != 200:
                raise self._api_client.build_exception(response)
            
            # Determine file type from content type header
            content_type = response.headers.get('Content-Type', '').lower()
            file_type = DatasourceFileType.tds if 'application/xml' in content_type else DatasourceFileType.tdsx
            
            # Return stream and file type
            return BytesIO(response.content), file_type
    
    @ApiVersionAttribute(2, 0)
    def update_data_source(self, 
                          session: TableauSession, 
                          datasource_id: str,
                          new_project_id: Optional[str] = None,
                          new_owner_id: Optional[str] = None,
                          new_certification_status: Optional[bool] = None,
                          new_certification_note: Optional[str] = None,
                          new_name: Optional[str] = None) -> DataSourceType:
        """
        Updates the owner, project or certification status of the specified data source. Available in versions 2.0+
        
        Args:
            session: TableauSession object
            datasource_id: The ID of the data source to update
            new_project_id: The ID of a project to add the data source to
            new_owner_id: The ID of a user to assign the data source to as owner
            new_certification_status: Whether the data source is certified
            new_certification_note: Note providing more information on certification
            new_name: The new name to give to the datasource
            
        Returns:
            DataSourceType: Updated data source information
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("datasource_id", datasource_id)
        )
        
        # Create datasource update object
        datasource = DataSourceType()
        datasource.name = new_name
        datasource.certification_note = new_certification_note
        
        if new_certification_status is not None:
            datasource.is_certified = new_certification_status
        
        if new_project_id:
            datasource.project = ProjectType()
            datasource.project.id = new_project_id
            
        if new_owner_id:
            datasource.owner = UserType()
            datasource.owner.id = new_owner_id
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/datasources/{datasource_id}")
        request_body = self._api_client.get_object_as_request_content(datasource)
        
        response_content = self._api_client.api_request(
            uri, "PUT", 200, session=session, body=request_body
        )
        
        # Parse response
        return self._api_client.get_response_as_object(response_content, DataSourceType)
    
    @ApiVersionAttribute(2, 3)
    def update_data_source_connection(self, 
                                    session: TableauSession, 
                                    datasource_id: str,
                                    connection_id: str,
                                    server_address: Optional[str] = None,
                                    server_port: Optional[str] = None,
                                    user_name: Optional[str] = None,
                                    password: Optional[str] = None,
                                    embed_password: Optional[bool] = None) -> ConnectionType:
        """
        Updates the server address, port, username, or password for the specified data source connection. 
        Versions 2.3+
        
        Args:
            session: TableauSession object
            datasource_id: The ID of the data source to update
            connection_id: The ID of the connection to update
            server_address: The new server for the connection
            server_port: The new port for the connection
            user_name: The new username for the connection
            password: The new password for the connection
            embed_password: True to embed the password; otherwise, False
            
        Returns:
            ConnectionType: Updated connection information
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("datasource_id", datasource_id),
            ("connection_id", connection_id)
        )
        
        # Create connection update object
        connection = ConnectionType()
        connection.server_address = server_address
        connection.server_port = server_port
        connection.user_name = user_name
        connection.password = password
        
        if embed_password is not None:
            connection.embed_password = embed_password
        
        # Build URI and make request
        uri = self._api_client.build_uri(
            f"sites/{session.site_id}/datasources/{datasource_id}/connections/{connection_id}"
        )
        request_body = self._api_client.get_object_as_request_content(connection)
        
        response_content = self._api_client.api_request(
            uri, "PUT", 200, session=session, body=request_body
        )
        
        # Parse response
        return self._api_client.get_response_as_object(response_content, ConnectionType)
    
    @ApiVersionAttribute(2, 8)
    def update_data_source_now(self, 
                              session: TableauSession, 
                              datasource_id: str) -> JobType:
        """
        Runs an extract refresh on the specified data source. Version 2.8+
        
        This method runs an extract refresh for the specified data source, with no need to associate 
        that extract refresh with a scheduled task. This method is the equivalent of selecting a data 
        source using the Tableau Server UI, and then selecting Refresh Extracts from the menu 
        (also known as a "manual refresh").
        
        Args:
            session: TableauSession object
            datasource_id: The ID of the data source to refresh
            
        Returns:
            JobType: Information about the refresh job
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("datasource_id", datasource_id)
        )
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/datasources/{datasource_id}/refresh")
        
        # Empty body for refresh request
        request_body = self._api_client.get_object_as_request_content(None)
        
        response_content = self._api_client.api_request(
            uri, "POST", 202, session=session, body=request_body
        )
        
        # Parse response
        return self._api_client.get_response_as_object(response_content, JobType)
    
    @ApiVersionAttribute(2, 0)
    def delete_data_source(self, 
                          session: TableauSession, 
                          datasource_id: str) -> None:
        """
        Deletes the specified data source from a site. When a data source is deleted, its associated 
        data connection is also deleted. Workbooks that use the data source are not deleted, but they 
        will no longer work properly. Available in versions 2.0+
        
        Args:
            session: TableauSession object
            datasource_id: The ID of the data source to delete
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("datasource_id", datasource_id)
        )
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/datasources/{datasource_id}")
        
        self._api_client.api_request(
            uri, "DELETE", 204, session=session, body=None
        )
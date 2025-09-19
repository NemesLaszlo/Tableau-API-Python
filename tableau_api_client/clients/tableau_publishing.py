from typing import Optional, List, TYPE_CHECKING
from io import BytesIO
from tableau_api_client.models.ts_api import (
    FileUploadType, DataSourceType, WorkbookType, ProjectType,
    ConnectionCredentialsType, ConnectionType
)
from tableau_api_client.models.tableau_session import TableauSession
from tableau_api_client.enums.datasource_file_type import DatasourceFileType
from tableau_api_client.enums.workbook_file_type import WorkbookFileType
from tableau_api_client.attributes.api_version_attribute import ApiVersionAttribute

if TYPE_CHECKING:
    from tableau_api_client.tableau_api_client import TableauApiClient


class TableauPublishingClient:
    """Client for Tableau Server file upload operations"""
    
    def __init__(self, api_client: "TableauApiClient"):
        """
        Initialize file uploads client
        
        Args:
            api_client: Instance of TableauApiClient
        """
        self._api_client = api_client
    
    @ApiVersionAttribute(2, 0)
    def initiate_file_upload(self, session: TableauSession) -> FileUploadType:
        """
        Initiates a file upload to Tableau Server. Available in all versions
        
        Args:
            session: TableauSession object
            
        Returns:
            FileUploadType: The file upload session object
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(("session", session))
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/fileUploads")
        
        response_content = self._api_client.api_request(
            uri, "POST", 201, session=session
        )
        
        # Parse response
        return self._api_client.get_response_as_object(response_content, FileUploadType)
    
    @ApiVersionAttribute(2, 0)
    def append_to_file_upload(
        self, 
        session: TableauSession, 
        file_upload_id: str, 
        source_stream: BytesIO, 
        max_bytes_to_read: int = 67108864
    ) -> FileUploadType:
        """
        Appends to a file upload to Tableau Server from an existing stream. Available in all versions
        
        Args:
            session: TableauSession object
            file_upload_id: The file upload session id to append to
            source_stream: The source stream to read and upload
            max_bytes_to_read: Maximum bytes to read (default: 67108864, max: 67108864)
            
        Returns:
            FileUploadType: The updated file upload session object
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("file_upload_id", file_upload_id),
            ("source_stream", source_stream)
        )
        self._api_client.check_parameters_between(
            ("max_bytes_to_read", max_bytes_to_read, 1, 67108864)
        )
        
        # Read the specified amount of data from the stream
        current_position = source_stream.tell()
        data = source_stream.read(max_bytes_to_read)
        
        # Create a new BytesIO with the data for upload
        upload_stream = BytesIO(data)
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/fileUploads/{file_upload_id}")
        
        # Prepare multipart content
        request_body = self._api_client.prepare_file_upload_content(
            xml_content="",  # Empty XML content for append operations
            file_stream=upload_stream
        )
        
        response_content = self._api_client.api_request(
            uri, "PUT", 200, session=session, body=request_body
        )
        
        # Parse response
        return self._api_client.get_response_as_object(response_content, FileUploadType)
    
    @ApiVersionAttribute(2, 0)
    def publish_datasource(
        self,
        session: TableauSession,
        file_upload_id: str,
        datasource_name: str,
        datasource_type: DatasourceFileType,
        project_id: str,
        connection_credentials: Optional[ConnectionCredentialsType] = None,
        overwrite: bool = False,
        append: bool = False
    ) -> DataSourceType:
        """
        Publishes a Datasource. Available in all versions
        
        Args:
            session: TableauSession object
            file_upload_id: The file upload session id that contains the uploaded file
            datasource_name: The name of the new datasource
            datasource_type: The type of the new datasource
            project_id: The ID of the project to assign the data source to
            connection_credentials: Connection credentials (optional)
            overwrite: True to overwrite existing datasource (default: False)
            append: True to append to existing datasource (default: False)
            
        Returns:
            DataSourceType: The published datasource object
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("file_upload_id", file_upload_id),
            ("datasource_name", datasource_name),
            ("project_id", project_id)
        )
        
        # Create project object
        project = ProjectType()
        project.id = project_id
        
        # Create datasource object
        datasource = DataSourceType()
        datasource.name = datasource_name
        datasource.project = project
        if connection_credentials:
            datasource.connection_credentials = connection_credentials
        
        # Build URI with query parameters
        uri = self._api_client.build_uri(
            f"sites/{session.site_id}/datasources",
            ("uploadSessionId", file_upload_id),
            ("datasourceType", datasource_type.value),
            ("overwrite", str(overwrite).lower()),
            ("append", str(append).lower())
        )
        
        # Prepare request content
        xml_content = self._api_client.get_object_as_request_content(datasource)
        request_body = self._api_client.prepare_file_upload_content(
            xml_content=xml_content,
            file_stream=None
        )
        
        response_content = self._api_client.api_request(
            uri, "POST", 201, session=session, body=request_body
        )
        
        # Parse response
        return self._api_client.get_response_as_object(response_content, DataSourceType)
    
    @ApiVersionAttribute(2, 0)
    def publish_workbook(
        self,
        session: TableauSession,
        file_upload_id: str,
        workbook_name: str,
        workbook_type: WorkbookFileType,
        project_id: str,
        show_tabs: bool = False,
        connections: Optional[List[ConnectionType]] = None,
        overwrite: bool = False
    ) -> WorkbookType:
        """
        Publishes a Workbook. Available in all versions
        
        Args:
            session: TableauSession object
            file_upload_id: The file upload session id that contains the uploaded file
            workbook_name: The name to assign to the workbook when saved on server
            workbook_type: twb or twbx to indicate workbook file type
            project_id: The ID of the project to assign the workbook to
            show_tabs: True to show views in tabs (default: False)
            connections: The connections to publish the workbooks with (optional)
            overwrite: True to overwrite existing workbook (default: False)
            
        Returns:
            WorkbookType: The published workbook object
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("file_upload_id", file_upload_id),
            ("workbook_name", workbook_name),
            ("project_id", project_id)
        )
        
        # Create project object
        project = ProjectType()
        project.id = project_id
        
        # Create workbook object
        workbook = WorkbookType()
        workbook.name = workbook_name
        workbook.show_tabs = show_tabs
        workbook.project = project
        if connections:
            workbook.connections = connections
        
        # Build URI with query parameters
        uri = self._api_client.build_uri(
            f"sites/{session.site_id}/workbooks",
            ("uploadSessionId", file_upload_id),
            ("workbookType", workbook_type.value),
            ("overwrite", str(overwrite).lower())
        )
        
        # Prepare request content
        xml_content = self._api_client.get_object_as_request_content(workbook)
        request_body = self._api_client.prepare_file_upload_content(
            xml_content=xml_content,
            file_stream=None
        )
        
        response_content = self._api_client.api_request(
            uri, "POST", 201, session=session, body=request_body
        )
        
        # Parse response
        return self._api_client.get_response_as_object(response_content, WorkbookType)
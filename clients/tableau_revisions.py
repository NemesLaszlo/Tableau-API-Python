from typing import List, Tuple, TYPE_CHECKING
from io import BytesIO
from models.tableau_session import TableauSession
from attributes.api_version_attribute import ApiVersionAttribute
from models.ts_api import PaginationType, RevisionType, RevisionListType

if TYPE_CHECKING:
    from tableau_api_client import TableauApiClient

class TableauRevisionsClient:
    """Client for Tableau Server revisions operations"""
    
    def __init__(self, api_client: "TableauApiClient"):
        """
        Initialize revisions client
        
        Args:
            api_client (TableauApiClient): Instance of TableauApiClient
        """
        self._api_client = api_client
    
    @ApiVersionAttribute(2, 3)
    def get_data_source_revisions(self,
                                 session: TableauSession,
                                 datasource_id: str,
                                 page_size: int = 100,
                                 page_number: int = 1) -> Tuple[PaginationType, List[RevisionType]]:
        """
        Returns a list of revision information (history) for the specified data source. Versions 2.3+
        
        Args:
            session (TableauSession): TableauSession object
            datasource_id (str): The ID of the data source to get revisions for
            page_size (int): The number of items to return in one response. The minimum is 1. The maximum is 1000. The default is 100
            page_number (int): The offset for paging. The default is 1
            
        Returns:
            Tuple[PaginationType, List[RevisionType]]: Tuple containing pagination information and list of revision objects
                
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing or out of range
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session),
            ("datasource_id", datasource_id)
        )
        self._api_client._check_parameters_between(
            ("page_size", page_size, 1, 1000),
            ("page_number", page_number, 1, float('inf'))
        )
        
        # Build query parameters
        url_params = [
            ("pageSize", str(page_size)),
            ("pageNumber", str(page_number))
        ]
        
        uri = self._api_client._build_uri(f"sites/{session.site_id}/datasources/{datasource_id}/revisions", *url_params)
        response_content = self._api_client._api_request(
            uri, "GET", 200, session=session
        )
        
        # Parse response to get both objects
        pagination_obj, revision_list_obj = self._api_client._get_response_as_objects(
            response_content, PaginationType, RevisionListType
        )
        
        revisions = revision_list_obj.revision if revision_list_obj else []
        return pagination_obj, revisions
    
    @ApiVersionAttribute(2, 3)
    def get_workbook_revisions(self,
                              session: TableauSession,
                              workbook_id: str,
                              page_size: int = 100,
                              page_number: int = 1) -> Tuple[PaginationType, List[RevisionType]]:
        """
        Returns a list of revision information (history) for the specified workbook. Versions 2.3+
        
        Args:
            session (TableauSession): TableauSession object
            workbook_id (str): The ID of the workbook to get revisions for
            page_size (int): The number of items to return in one response. The minimum is 1. The maximum is 1000. The default is 100
            page_number (int): The offset for paging. The default is 1
            
        Returns:
            Tuple[PaginationType, List[RevisionType]]: Tuple containing pagination information and list of revision objects
                
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing or out of range
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session),
            ("workbook_id", workbook_id)
        )
        self._api_client._check_parameters_between(
            ("page_size", page_size, 1, 1000),
            ("page_number", page_number, 1, float('inf'))
        )
        
        # Build query parameters
        url_params = [
            ("pageSize", str(page_size)),
            ("pageNumber", str(page_number))
        ]
        
        uri = self._api_client._build_uri(f"sites/{session.site_id}/workbooks/{workbook_id}/revisions", *url_params)
        response_content = self._api_client._api_request(
            uri, "GET", 200, session=session
        )
        
        # Parse response to get both objects
        pagination_obj, revision_list_obj = self._api_client._get_response_as_objects(
            response_content, PaginationType, RevisionListType
        )
        
        revisions = revision_list_obj.revision if revision_list_obj else []
        return pagination_obj, revisions
    
    @ApiVersionAttribute(2, 3)
    def download_data_source_revision(self,
                                     session: TableauSession,
                                     datasource_id: str,
                                     revision_number: int,
                                     include_extract: bool = True) -> BytesIO:
        """
        Downloads a specific version of a data source in .tdsx format. Returns data stream. Versions 2.3+
        
        Args:
            session (TableauSession): TableauSession object
            datasource_id (str): The ID of the data source to download
            revision_number (int): The revision number of the data source to download
            include_extract (bool): The extract-value is a Boolean value (False or True). When the data source specified for download has an extract, if you add the parameter ?includeExtract=False, the extract is not included when you download the data source. You can use this parameter to improve performance if you are downloading workbooks or data sources that have large extracts
            
        Returns:
            BytesIO: BytesIO stream containing the data source file
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session),
            ("datasource_id", datasource_id)
        )
        
        uri = self._api_client._build_uri(
            f"sites/{session.site_id}/datasources/{datasource_id}/revisions/{revision_number}/content",
            ("includeExtract", str(include_extract).lower())
        )
        
        # Get the raw response content as bytes
        response_content = self._api_client._api_request(
            uri, "GET", 200, session=session, return_bytes=True
        )
        
        # response_content is now bytes, convert directly to BytesIO
        return BytesIO(response_content)
    
    @ApiVersionAttribute(2, 3)
    def download_workbook_revision(self,
                                  session: TableauSession,
                                  workbook_id: str,
                                  revision_number: int,
                                  include_extract: bool = True) -> BytesIO:
        """
        Downloads a specific version of a workbook in .twb or .twbx format. Returns data stream. Versions 2.3+
        
        Args:
            session (TableauSession): TableauSession object
            workbook_id (str): The ID of the workbook to download
            revision_number (int): The revision number of the workbook to download
            include_extract (bool): The extract-value is a Boolean value (False or True). When the data source specified for download has an extract, if you add the parameter ?includeExtract=False, the extract is not included when you download the data source. You can use this parameter to improve performance if you are downloading workbooks or data sources that have large extracts
            
        Returns:
            BytesIO: BytesIO stream containing the workbook file
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session),
            ("workbook_id", workbook_id)
        )
        
        uri = self._api_client._build_uri(
            f"sites/{session.site_id}/workbooks/{workbook_id}/revisions/{revision_number}/content",
            ("includeExtract", str(include_extract).lower())
        )
        
        # Get the raw response content as bytes
        response_content = self._api_client._api_request(
            uri, "GET", 200, session=session, return_bytes=True
        )
        
        # response_content is now bytes, convert directly to BytesIO
        return BytesIO(response_content)
    
    @ApiVersionAttribute(2, 3)
    def remove_data_source_revision(self,
                                   session: TableauSession,
                                   datasource_id: str,
                                   revision_number: int) -> None:
        """
        Removes a specific version of a data source. Versions 2.3+
        
        Args:
            session (TableauSession): TableauSession object
            datasource_id (str): The ID of the data source to remove the revision for
            revision_number (int): The revision number of the data source to remove
            
        Returns:
            None
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session),
            ("datasource_id", datasource_id)
        )
        
        uri = self._api_client._build_uri(f"sites/{session.site_id}/datasources/{datasource_id}/revisions/{revision_number}")
        
        self._api_client._api_request(
            uri, "DELETE", 204, session=session
        )
    
    @ApiVersionAttribute(2, 3)
    def remove_workbook_revision(self,
                                session: TableauSession,
                                workbook_id: str,
                                revision_number: int) -> None:
        """
        Removes a specific version of a workbook. Versions 2.3+
        
        Args:
            session (TableauSession): TableauSession object
            workbook_id (str): The ID of the workbook to remove the revision for
            revision_number (int): The revision number of the workbook to remove
            
        Returns:
            None
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session),
            ("workbook_id", workbook_id)
        )
        
        uri = self._api_client._build_uri(f"sites/{session.site_id}/workbooks/{workbook_id}/revisions/{revision_number}")
        
        self._api_client._api_request(
            uri, "DELETE", 204, session=session
        )
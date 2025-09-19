from typing import Optional, List, Tuple, TYPE_CHECKING, IO
from io import BytesIO
from tableau_api_client.models.ts_api import (
    ProjectType, TagType, TagListType, UserType, ViewType, ViewListType, WorkbookType, WorkbookListType,
    ConnectionType, ConnectionListType, JobType, PaginationType
)
from tableau_api_client.models.tableau_session import TableauSession
from tableau_api_client.attributes.api_version_attribute import ApiVersionAttribute
from tableau_api_client.enums import (
    TableauImageWidth, PageType, Orientation, PageOrientation, WorkbookFileType
)

if TYPE_CHECKING:
    from tableau_api_client.tableau_api_client import TableauApiClient


class TableauWorkbooksViewsClient:
    """Client for Tableau Server workbooks and views operations"""
    
    def __init__(self, api_client: "TableauApiClient"):
        """
        Initialize workbooks/views client
        
        Args:
            api_client: Instance of TableauApiClient
        """
        self._api_client = api_client

    # Tags methods
    @ApiVersionAttribute(1, 0)
    def add_tags_to_view(self, 
                        session: TableauSession, 
                        view_id: str, 
                        tags_to_add: List[str]) -> List[TagType]:
        """
        Adds one or more tags to the specified view. Available in all versions.
        
        Args:
            session: Current TableauSession
            view_id: The ID of the view to add tags to
            tags_to_add: List of tag names to add to the view
            
        Returns:
            List[TagType]: List of tags that were added
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing or empty
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session), 
            ("view_id", view_id), 
            ("tags_to_add", tags_to_add)
        )
        self._api_client.check_empty_arrays(("tags_to_add", tags_to_add))
        
        # Create tag list object
        tag_list = TagListType()
        tag_list.tag = [TagType(label=tag) for tag in tags_to_add]
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/views/{view_id}/tags")
        request_body = self._api_client.get_object_as_request_content(tag_list)
        
        response_content = self._api_client.api_request(
            uri, "PUT", 200, session=session, body=request_body
        )
        
        # Parse response
        response_tags = self._api_client.get_response_as_object(response_content, TagListType)
        return response_tags.tag if response_tags.tag else []

    @ApiVersionAttribute(1, 0)
    def add_tags_to_workbook(self, 
                            session: TableauSession, 
                            workbook_id: str, 
                            tags_to_add: List[str]) -> List[TagType]:
        """
        Adds one or more tags to the specified workbook. Available in all versions.
        
        Args:
            session: Current TableauSession
            workbook_id: The ID of the workbook to add tags to
            tags_to_add: List of tag names to add to the workbook
            
        Returns:
            List[TagType]: List of tags that were added
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing or empty
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session), 
            ("workbook_id", workbook_id), 
            ("tags_to_add", tags_to_add)
        )
        self._api_client.check_empty_arrays(("tags_to_add", tags_to_add))
        
        # Create tag list object
        tag_list = TagListType()
        tag_list.tag = [TagType(label=tag) for tag in tags_to_add]
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/workbooks/{workbook_id}/tags")
        request_body = self._api_client.get_object_as_request_content(tag_list)
        
        response_content = self._api_client.api_request(
            uri, "PUT", 200, session=session, body=request_body
        )
        
        # Parse response
        response_tags = self._api_client.get_response_as_object(response_content, TagListType)
        return response_tags.tag if response_tags.tag else []

    @ApiVersionAttribute(1, 0)
    def delete_tag_from_view(self, 
                            session: TableauSession, 
                            view_id: str, 
                            tag_name: str) -> None:
        """
        Deletes a tag from the specified view. Available in all versions.
        
        Args:
            session: Current TableauSession
            view_id: The ID of the view to remove the tag from
            tag_name: The name of the tag to remove from the view
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session), 
            ("view_id", view_id), 
            ("tag_name", tag_name)
        )
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/views/{view_id}/tags/{tag_name}")
        
        self._api_client.api_request(
            uri, "DELETE", 204, session=session, body=None
        )

    @ApiVersionAttribute(1, 0)
    def delete_tag_from_workbook(self, 
                                session: TableauSession, 
                                workbook_id: str, 
                                tag_name: str) -> None:
        """
        Deletes a tag from the specified workbook. Available in all versions.
        
        Args:
            session: Current TableauSession
            workbook_id: The ID of the workbook to remove the tag from
            tag_name: The name of the tag to remove from the workbook
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session), 
            ("workbook_id", workbook_id), 
            ("tag_name", tag_name)
        )
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/workbooks/{workbook_id}/tags/{tag_name}")
        
        self._api_client.api_request(
            uri, "DELETE", 204, session=session, body=None
        )

    # Views query methods
    @ApiVersionAttribute(1, 0)
    def query_views_for_workbook(self, 
                                session: TableauSession, 
                                workbook_id: str, 
                                include_usage_statistics: bool = False) -> List[ViewType]:
        """
        Returns all the views for the specified workbook, optionally including usage statistics. Available in all versions.
        
        Args:
            session: Current TableauSession
            workbook_id: The ID of the workbook to get the views for
            include_usage_statistics: True to return usage statistics. The default is False
            
        Returns:
            List[ViewType]: List of views in the workbook
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session), 
            ("workbook_id", workbook_id)
        )
        
        # Build URI with optional parameters
        params = []
        if include_usage_statistics:
            params.append(("includeUsageStatistics", str(include_usage_statistics).lower()))
        
        uri = self._api_client.build_uri(f"sites/{session.site_id}/workbooks/{workbook_id}/views", *params)
        
        response_content = self._api_client.api_request(
            uri, "GET", 200, session=session, body=None
        )
        
        # Parse response
        view_list = self._api_client.get_response_as_object(response_content, ViewListType)
        return view_list.view if view_list.view else []

    @ApiVersionAttribute(3, 0)
    def query_view(self, 
                  session: TableauSession, 
                  view_id: str, 
                  include_usage_statistics: bool = False) -> ViewType:
        """
        Gets the details of a specific view. Versions 3.0+
        
        Args:
            session: Current TableauSession
            view_id: The ID of the view to get the details for
            include_usage_statistics: True to return usage statistics. The default is False
            
        Returns:
            ViewType: The view details
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session), 
            ("view_id", view_id)
        )
        
        # Build URI with optional parameters
        params = []
        if include_usage_statistics:
            params.append(("includeUsageStatistics", str(include_usage_statistics).lower()))
        
        uri = self._api_client.build_uri(f"sites/{session.site_id}/views/{view_id}", *params)
        
        response_content = self._api_client.api_request(
            uri, "GET", 200, session=session, body=None
        )
        
        # Parse response
        return self._api_client.get_response_as_object(response_content, ViewType)

    # View data export methods
    @ApiVersionAttribute(2, 8)
    def query_view_data(self, 
                       session: TableauSession, 
                       view_id: str, 
                       filters: Optional[List[Tuple[str, str]]] = None) -> IO[bytes]:
        """
        Returns a data stream of the specified view rendered as data in CSV format. Versions 2.8+
        
        Args:
            session: Current TableauSession
            view_id: The ID of the view to render as data
            filters: Optional list of (filter_name, filter_value) tuples to apply to the view
            
        Returns:
            IO[bytes]: Stream containing the CSV data
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session), 
            ("view_id", view_id)
        )
        
        # Validate filters
        params = []
        if filters:
            for filter_name, filter_value in filters:
                self._api_client.check_null_parameters(
                    ("filter_name", filter_name), 
                    ("filter_value", filter_value)
                )
                # Remove 'vf_' prefix if present and add to params
                clean_name = filter_name[3:] if filter_name.lower().startswith('vf_') else filter_name
                params.append((clean_name, filter_value))
        
        uri = self._api_client.build_uri(f"sites/{session.site_id}/views/{view_id}/data", *params)
        
        response_content = self._api_client.api_request(
            uri, "GET", 200, session=session, body=None
        )
        
        return BytesIO(response_content)

    @ApiVersionAttribute(2, 5)
    def query_view_image(self, 
                        session: TableauSession, 
                        view_id: str, 
                        image_width: TableauImageWidth = TableauImageWidth.Default_784px,
                        filters: Optional[List[Tuple[str, str]]] = None) -> IO[bytes]:
        """
        Returns an image of the specified view as data stream. Versions 2.5+
        
        Args:
            session: Current TableauSession
            view_id: The ID of the view to return an image for
            image_width: The resolution of the image
            filters: Optional list of (filter_name, filter_value) tuples to apply to the view
            
        Returns:
            IO[bytes]: Stream containing the image data
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session), 
            ("view_id", view_id)
        )
        
        # Build parameters
        params = []
        if image_width == TableauImageWidth.High_1568:
            params.append(("resolution", "high"))
        
        # Validate and add filters
        if filters:
            for filter_name, filter_value in filters:
                self._api_client.check_null_parameters(
                    ("filter_name", filter_name), 
                    ("filter_value", filter_value)
                )
                # Add 'vf_' prefix if not present
                prefixed_name = filter_name if filter_name.lower().startswith('vf_') else f"vf_{filter_name}"
                params.append((prefixed_name, filter_value))
        
        uri = self._api_client.build_uri(f"sites/{session.site_id}/views/{view_id}/image", *params)
        
        response_content = self._api_client.api_request(
            uri, "GET", 200, session=session, body=None
        )
        
        return BytesIO(response_content)

    @ApiVersionAttribute(2, 8)
    def query_view_pdf(self, 
                      session: TableauSession, 
                      view_id: str, 
                      page_type: PageType = PageType.Legal,
                      orientation: Orientation = Orientation.Portrait,
                      filters: Optional[List[Tuple[str, str]]] = None) -> IO[bytes]:
        """
        Returns a specified view rendered as a PDF file as data stream. Versions 2.8+
        
        Args:
            session: Current TableauSession
            view_id: The ID of the view to render as a PDF file
            page_type: The page type for the PDF
            orientation: The orientation for the PDF
            filters: Optional list of (filter_name, filter_value) tuples to apply to the view
            
        Returns:
            IO[bytes]: Stream containing the PDF data
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session), 
            ("view_id", view_id)
        )
        
        # Build parameters
        params = [
            ("type", page_type.name),
            ("orientation", orientation.name)
        ]
        
        # Validate and add filters
        if filters:
            for filter_name, filter_value in filters:
                self._api_client.check_null_parameters(
                    ("filter_name", filter_name), 
                    ("filter_value", filter_value)
                )
                # Add 'vf_' prefix if not present
                prefixed_name = filter_name if filter_name.lower().startswith('vf_') else f"vf_{filter_name}"
                params.append((prefixed_name, filter_value))
        
        uri = self._api_client.build_uri(f"sites/{session.site_id}/views/{view_id}/pdf", *params)
        
        response_content = self._api_client.api_request(
            uri, "GET", 200, session=session, body=None
        )
        
        return BytesIO(response_content)

    @ApiVersionAttribute(3, 9)
    def query_view_crosstab_excel(self, 
                                 session: TableauSession, 
                                 view_id: str, 
                                 filters: Optional[List[Tuple[str, str]]] = None) -> IO[bytes]:
        """
        Returns a specified view rendered as a .xlsx file containing crosstab data. Versions 3.9+
        
        Args:
            session: Current TableauSession
            view_id: The ID of the view to render as a .xlsx file
            filters: Optional list of (filter_name, filter_value) tuples to apply to the view
            
        Returns:
            IO[bytes]: Stream containing the Excel data
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session), 
            ("view_id", view_id)
        )
        
        # Build parameters
        params = []
        
        # Validate and add filters
        if filters:
            for filter_name, filter_value in filters:
                self._api_client.check_null_parameters(
                    ("filter_name", filter_name), 
                    ("filter_value", filter_value)
                )
                # Add 'vf_' prefix if not present
                prefixed_name = filter_name if filter_name.lower().startswith('vf_') else f"vf_{filter_name}"
                params.append((prefixed_name, filter_value))
        
        uri = self._api_client.build_uri(f"sites/{session.site_id}/views/{view_id}/crosstab/excel", *params)
        
        response_content = self._api_client.api_request(
            uri, "GET", 200, session=session, body=None
        )
        
        return BytesIO(response_content)

    @ApiVersionAttribute(1, 0)
    def query_view_preview_image(self, 
                                session: TableauSession, 
                                workbook_id: str, 
                                view_id: str) -> IO[bytes]:
        """
        Returns the thumbnail image for the specified view as data stream. Available in all versions.
        
        Args:
            session: Current TableauSession
            workbook_id: The ID of the workbook that the view belongs to
            view_id: The ID of the view to return an image for
            
        Returns:
            IO[bytes]: Stream containing the thumbnail image
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session), 
            ("workbook_id", workbook_id), 
            ("view_id", view_id)
        )
        
        uri = self._api_client.build_uri(f"sites/{session.site_id}/workbooks/{workbook_id}/views/{view_id}/previewImage")
        
        response_content = self._api_client.api_request(
            uri, "GET", 200, session=session, body=None
        )
        
        return BytesIO(response_content)

    # Workbook methods
    @ApiVersionAttribute(1, 0)
    def query_workbook(self, 
                      session: TableauSession, 
                      workbook_id: str) -> WorkbookType:
        """
        Returns information about the specified workbook, including information about views and tags. Available in all versions.
        
        Args:
            session: Current TableauSession
            workbook_id: The ID of the workbook to return information about
            
        Returns:
            WorkbookType: The workbook information
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session), 
            ("workbook_id", workbook_id)
        )
        
        uri = self._api_client.build_uri(f"sites/{session.site_id}/workbooks/{workbook_id}")
        
        response_content = self._api_client.api_request(
            uri, "GET", 200, session=session, body=None
        )
        
        return self._api_client.get_response_as_object(response_content, WorkbookType)

    @ApiVersionAttribute(2, 0)
    def query_workbook_connections(self, 
                                  session: TableauSession, 
                                  workbook_id: str) -> List[ConnectionType]:
        """
        Returns a list of data connections for the specific workbook. Versions 2.0+
        
        Args:
            session: Current TableauSession
            workbook_id: The ID of the workbook to return connection information about
            
        Returns:
            List[ConnectionType]: List of connections for the workbook
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session), 
            ("workbook_id", workbook_id)
        )
        
        uri = self._api_client.build_uri(f"sites/{session.site_id}/workbooks/{workbook_id}/connections")
        
        response_content = self._api_client.api_request(
            uri, "GET", 200, session=session, body=None
        )
        
        connection_list = self._api_client.get_response_as_object(response_content, ConnectionListType)
        return connection_list.connection if connection_list.connection else []

    @ApiVersionAttribute(1, 0)
    def query_workbook_preview_image(self, 
                                    session: TableauSession, 
                                    workbook_id: str) -> IO[bytes]:
        """
        Returns the thumbnail image for the specified workbook as data stream. Available in all versions.
        
        Args:
            session: Current TableauSession
            workbook_id: The ID of the workbook to return an image for
            
        Returns:
            IO[bytes]: Stream containing the thumbnail image
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session), 
            ("workbook_id", workbook_id)
        )
        
        uri = self._api_client.build_uri(f"sites/{session.site_id}/workbooks/{workbook_id}/previewImage")
        
        response_content = self._api_client.api_request(
            uri, "GET", 200, session=session, body=None
        )
        
        return BytesIO(response_content)

    @ApiVersionAttribute(3, 4)
    def query_workbook_pdf(self, 
                          session: TableauSession, 
                          workbook_id: str,
                          orientation: PageOrientation = PageOrientation.Portrait,
                          page_type: PageType = PageType.Legal) -> IO[bytes]:
        """
        Returns the PDF for the specified workbook as data stream. Versions 3.4+
        
        Args:
            session: Current TableauSession
            workbook_id: The ID of the workbook to return a PDF for
            orientation: The orientation for the PDF
            page_type: The page type for the PDF
            
        Returns:
            IO[bytes]: Stream containing the PDF data
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session), 
            ("workbook_id", workbook_id)
        )
        
        params = [
            ("type", page_type.name),
            ("orientation", orientation.name)
        ]
        
        uri = self._api_client.build_uri(f"sites/{session.site_id}/workbooks/{workbook_id}/pdf", *params)
        
        response_content = self._api_client.api_request(
            uri, "GET", 200, session=session, body=None
        )
        
        return BytesIO(response_content)

    @ApiVersionAttribute(2, 3)
    def query_workbooks_for_site(self, 
                                session: TableauSession,
                                page_size: int = 100,
                                page_number: int = 1,
                                fields_expression: Optional[str] = None,
                                filter_expression: Optional[str] = None,
                                sort_expression: Optional[str] = None) -> Tuple[PaginationType, List[WorkbookType]]:
        """
        Queries workbooks for specified site. Versions 2.3+
        
        Args:
            session: Current TableauSession
            page_size: The number of items to return in one response (1-1000, default: 100)
            page_number: The offset for paging (default: 1)
            fields_expression: Expression to specify available fields to return
            filter_expression: Expression to specify subset of workbooks to return
            sort_expression: Expression to specify sort order
            
        Returns:
            Tuple[PaginationType, List[WorkbookType]]: Pagination info and list of workbooks
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If parameters are out of range
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(("session", session))
        self._api_client.check_parameters_between(
            ("page_size", page_size, 1, 1000),
            ("page_number", page_number, 1, 2147483647)
        )
        
        # Build parameters
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
        
        uri = self._api_client.build_uri(f"sites/{session.site_id}/workbooks", *params)
        
        response_content = self._api_client.api_request(
            uri, "GET", 200, session=session, body=None
        )
        
        # Parse response using the same pattern as query_data_sources
        pagination, workbook_list = self._api_client.get_response_as_objects(
            response_content, PaginationType, WorkbookListType
        )
        
        workbooks = workbook_list.workbook if workbook_list and workbook_list.workbook else []
        return pagination, workbooks


    @ApiVersionAttribute(1, 0)
    def query_workbooks_for_user(self, 
                                session: TableauSession,
                                user_id: str,
                                is_owner: bool = False,
                                page_size: int = 100,
                                page_number: int = 1) -> Tuple[PaginationType, List[WorkbookType]]:
        """
        Returns workbooks that the specified user owns or has Read permissions for. Available in all versions.
        
        Args:
            session: Current TableauSession
            user_id: The ID of the user to get workbooks for
            is_owner: True to return only owned workbooks, False for all accessible workbooks
            page_size: The number of items to return in one response (1-1000, default: 100)
            page_number: The offset for paging (default: 1)
            
        Returns:
            Tuple[PaginationType, List[WorkbookType]]: Pagination info and list of workbooks
            
        Raises:
            TableauRequestException: If the request fails
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
        
        params = [
            ("ownedBy", str(is_owner).lower()),
            ("pageSize", str(page_size)),
            ("pageNumber", str(page_number))
        ]
        
        uri = self._api_client.build_uri(f"sites/{session.site_id}/users/{user_id}/workbooks", *params)
        
        response_content = self._api_client.api_request(
            uri, "GET", 200, session=session, body=None
        )
        
        # Parse response using the same pattern as query_data_sources
        pagination, workbook_list = self._api_client.get_response_as_objects(
            response_content, PaginationType, WorkbookListType
        )
        
        workbooks = workbook_list.workbook if workbook_list and workbook_list.workbook else []
        return pagination, workbooks

    @ApiVersionAttribute(2, 0)
    def download_workbook(self, 
                         session: TableauSession, 
                         workbook_id: str,
                         include_extract: bool = True) -> Tuple[IO[bytes], WorkbookFileType]:
        """
        Downloads a workbook in .twb or .twbx format. Versions 2.0+
        
        Args:
            session: Current TableauSession
            workbook_id: The ID of the workbook to download
            include_extract: Whether to include extracts in the download
            
        Returns:
            Tuple[IO[bytes], WorkbookFileType]: Stream containing workbook data and file type
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session), 
            ("workbook_id", workbook_id)
        )
        
        params = [("includeExtract", str(include_extract).lower())]
        
        uri = self._api_client.build_uri(f"sites/{session.site_id}/workbooks/{workbook_id}/content", *params)
        
        response_content = self._api_client.api_request(
            uri, "GET", 200, session=session, body=None
        )
        
        # You might need to get content type from response headers
        # This assumes your API client can provide the content type somehow
        content_type = getattr(response_content, 'content_type', None) or 'application/octet-stream'
        
        # Determine file type based on content type
        file_type = (WorkbookFileType.twb if content_type and 'application/xml' in content_type.lower() 
                    else WorkbookFileType.twbx)
        
        return BytesIO(response_content), file_type

    @ApiVersionAttribute(2, 0)
    def update_workbook(self, 
                       session: TableauSession, 
                       workbook_id: str,
                       new_name: Optional[str] = None,
                       new_show_tabs: Optional[bool] = None,
                       new_project_id: Optional[str] = None,
                       new_owner_id: Optional[str] = None) -> WorkbookType:
        """
        Modifies an existing workbook, allowing you to change owner or project and show tabs setting. Versions 2.0+
        
        Args:
            session: Current TableauSession
            workbook_id: The ID of the workbook to update
            new_name: New name for the workbook
            new_show_tabs: Whether the workbook shows views in tabs
            new_project_id: The ID of a project to assign the workbook to
            new_owner_id: The ID of a user to assign the workbook to as owner
            
        Returns:
            WorkbookType: The updated workbook
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session), 
            ("workbook_id", workbook_id)
        )
        
        # Create workbook update object  
        workbook = WorkbookType()
        workbook.name = new_name
        
        if new_show_tabs is not None:
            workbook.show_tabs = str(new_show_tabs).lower()
      
        if new_project_id:
            workbook.project = ProjectType()
            workbook.project.id = new_project_id
            
        if new_owner_id:
            workbook.owner = UserType()
            workbook.owner.id = new_owner_id
        
        uri = self._api_client.build_uri(f"sites/{session.site_id}/workbooks/{workbook_id}")
        request_body = self._api_client.get_object_as_request_content(workbook)
        
        response_content = self._api_client.api_request(
            uri, "PUT", 200, session=session, body=request_body
        )
        
        return self._api_client.get_response_as_object(response_content, WorkbookType)

    @ApiVersionAttribute(2, 3)
    def update_workbook_connection(self, 
                                  session: TableauSession, 
                                  workbook_id: str,
                                  connection_id: str,
                                  server_address: Optional[str] = None,
                                  server_port: Optional[str] = None,
                                  user_name: Optional[str] = None,
                                  password: Optional[str] = None,
                                  embed_password: Optional[bool] = None) -> ConnectionType:
        """
        Updates the server address, port, username, or password for the specified workbook connection. Versions 2.3+
        
        Args:
            session: Current TableauSession
            workbook_id: The ID of the workbook to update
            connection_id: The ID of the connection to update
            server_address: The new server for the connection
            server_port: The new port for the connection
            user_name: The new username for the connection
            password: The new password for the connection
            embed_password: True to embed the password; otherwise, False
            
        Returns:
            ConnectionType: The updated connection
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session), 
            ("workbook_id", workbook_id), 
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
        else:
            connection.embed_password = False
        
        uri = self._api_client.build_uri(f"sites/{session.site_id}/workbooks/{workbook_id}/connections/{connection_id}")
        request_body = self._api_client.get_object_as_request_content(connection)
        
        response_content = self._api_client.api_request(
            uri, "PUT", 200, session=session, body=request_body
        )
        
        return self._api_client.get_response_as_object(response_content, ConnectionType)

    @ApiVersionAttribute(2, 8)
    def update_workbook_now(self, 
                           session: TableauSession, 
                           workbook_id: str) -> JobType:
        """
        Runs an extract refresh on the specified workbook. Versions 2.8+
        
        This method runs an extract refresh for the specified workbook, with no need to associate 
        that extract refresh with a scheduled task. This is equivalent to a "manual refresh".
        
        Args:
            session: Current TableauSession
            workbook_id: The ID of the workbook to refresh
            
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
            ("workbook_id", workbook_id)
        )
        
        uri = self._api_client.build_uri(f"sites/{session.site_id}/workbooks/{workbook_id}/refresh")
        
        response_content = self._api_client.api_request(
            uri, "POST", 202, session=session, body=None
        )
        
        return self._api_client.get_response_as_object(response_content, JobType)

    @ApiVersionAttribute(2, 0)
    def delete_workbook(self, 
                       session: TableauSession, 
                       workbook_id: str) -> None:
        """
        Deletes a workbook. When a workbook is deleted, all assets are also deleted. Versions 2.0+
        
        Args:
            session: Current TableauSession
            workbook_id: The ID of the workbook to remove
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session), 
            ("workbook_id", workbook_id)
        )
        
        uri = self._api_client.build_uri(f"sites/{session.site_id}/workbooks/{workbook_id}")
        
        self._api_client.api_request(
            uri, "DELETE", 204, session=session, body=None
        )
from typing import List, TYPE_CHECKING
from models.ts_api import (
    FavoriteType, FavoriteListType, DataSourceType, ProjectType, 
    WorkbookType, ViewType
)
from models.tableau_session import TableauSession
from attributes.api_version_attribute import ApiVersionAttribute

if TYPE_CHECKING:
    from tableau_api_client import TableauApiClient


class TableauFavoritesClient:
    """Client for Tableau Server favorites operations"""
    
    def __init__(self, api_client: "TableauApiClient"):
        """
        Initialize favorites client
        
        Args:
            api_client: Instance of TableauApiClient
        """
        self._api_client = api_client
    
    @ApiVersionAttribute(2, 3)
    def add_data_source_to_favorites(self, 
                                   session: TableauSession, 
                                   user_id: str,
                                   datasource_id: str, 
                                   favorite_label: str) -> FavoriteType:
        """
        Adds the specified data source to the user's favorites. Versions 2.3+
        
        Args:
            session: TableauSession object
            user_id: The ID of the user to add the favorite for
            datasource_id: The ID of the data source to add as a favorite
            favorite_label: A label to assign to the favorite. This value is displayed when you search for favorites on the server
            
        Returns:
            FavoriteType: Information about the added favorite
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("user_id", user_id),
            ("datasource_id", datasource_id),
            ("favorite_label", favorite_label)
        )
        
        # Create favorite object with data source item
        favorite = FavoriteType()
        favorite.label = favorite_label
        
        # Create datasource reference
        datasource = DataSourceType()
        datasource.id = datasource_id
        favorite.datasource = datasource
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/favorites/{user_id}")
        request_body = self._api_client.get_object_as_request_content(favorite)
        
        response_content = self._api_client.api_request(
            uri, "PUT", 200, session=session, body=request_body
        )
        
        # Parse response
        return self._api_client.get_response_as_object(response_content, FavoriteType)
    
    @ApiVersionAttribute(3, 1)
    def add_project_to_favorites(self, 
                               session: TableauSession, 
                               user_id: str,
                               project_id: str, 
                               favorite_label: str) -> FavoriteType:
        """
        Adds the specified project to the user's favorites. Versions 3.1+
        
        Args:
            session: TableauSession object
            user_id: The ID of the user to add the favorite for
            project_id: The ID of the project to add as a favorite
            favorite_label: A label to assign to the favorite. This value is displayed when you search for favorites on the server
            
        Returns:
            FavoriteType: Information about the added favorite
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("user_id", user_id),
            ("project_id", project_id),
            ("favorite_label", favorite_label)
        )
        
        # Create favorite object with project item
        favorite = FavoriteType()
        favorite.label = favorite_label
        
        # Create project reference
        project = ProjectType()
        project.id = project_id
        favorite.project = project
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/favorites/{user_id}")
        request_body = self._api_client.get_object_as_request_content(favorite)
        
        response_content = self._api_client.api_request(
            uri, "PUT", 200, session=session, body=request_body
        )
        
        # Parse response
        return self._api_client.get_response_as_object(response_content, FavoriteType)
    
    @ApiVersionAttribute(2, 0)
    def add_workbook_to_favorites(self, 
                                session: TableauSession, 
                                user_id: str,
                                workbook_id: str, 
                                favorite_label: str) -> FavoriteType:
        """
        Adds the specified workbook to the user's favorites. Available in versions 2.0+
        
        Args:
            session: TableauSession object
            user_id: The ID of the user to add the favorite for
            workbook_id: The ID of the workbook to add as a favorite
            favorite_label: A label to assign to the favorite. This value is displayed when you search for favorites on the server
            
        Returns:
            FavoriteType: Information about the added favorite
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("user_id", user_id),
            ("workbook_id", workbook_id),
            ("favorite_label", favorite_label)
        )
        
        # Create favorite object with workbook item
        favorite = FavoriteType()
        favorite.label = favorite_label
        
        # Create workbook reference
        workbook = WorkbookType()
        workbook.id = workbook_id
        favorite.workbook = workbook
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/favorites/{user_id}")
        request_body = self._api_client.get_object_as_request_content(favorite)
        
        response_content = self._api_client.api_request(
            uri, "PUT", 200, session=session, body=request_body
        )
        
        # Parse response
        return self._api_client.get_response_as_object(response_content, FavoriteType)
    
    @ApiVersionAttribute(2, 0)
    def add_view_to_favorites(self, 
                            session: TableauSession, 
                            user_id: str,
                            view_id: str, 
                            favorite_label: str) -> FavoriteType:
        """
        Adds the specified view to the user's favorites. Available in versions 2.0+
        
        Args:
            session: TableauSession object
            user_id: The ID of the user to add the favorite for
            view_id: The ID of the view to add as a favorite
            favorite_label: A label to assign to the favorite. This value is displayed when you search for favorites on the server
            
        Returns:
            FavoriteType: Information about the added favorite
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("user_id", user_id),
            ("view_id", view_id),
            ("favorite_label", favorite_label)
        )
        
        # Create favorite object with view item
        favorite = FavoriteType()
        favorite.label = favorite_label
        
        # Create view reference
        view = ViewType()
        view.id = view_id
        favorite.view = view
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/favorites/{user_id}")
        request_body = self._api_client.get_object_as_request_content(favorite)
        
        response_content = self._api_client.api_request(
            uri, "PUT", 200, session=session, body=request_body
        )
        
        # Parse response
        return self._api_client.get_response_as_object(response_content, FavoriteType)
    
    @ApiVersionAttribute(2, 3)
    def delete_data_source_from_favorites(self, 
                                        session: TableauSession, 
                                        user_id: str, 
                                        datasource_id: str) -> None:
        """
        Deletes the specified data source from the user's favorites. Versions 2.3+
        
        Args:
            session: TableauSession object
            user_id: The ID of the user to remove the favorite from
            datasource_id: The ID of the data source to remove from the user's favorites
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("user_id", user_id),
            ("datasource_id", datasource_id)
        )
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/favorites/{user_id}/datasources/{datasource_id}")
        
        self._api_client.api_request(
            uri, "DELETE", 204, session=session, body=None
        )
    
    @ApiVersionAttribute(3, 1)
    def delete_project_from_favorites(self, 
                                    session: TableauSession, 
                                    user_id: str, 
                                    project_id: str) -> None:
        """
        Deletes the specified project from the user's favorites. Versions 3.1+
        
        Args:
            session: TableauSession object
            user_id: The ID of the user to remove the favorite from
            project_id: The ID of the project to remove from the user's favorites
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("user_id", user_id),
            ("project_id", project_id)
        )
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/favorites/{user_id}/projects/{project_id}")
        
        self._api_client.api_request(
            uri, "DELETE", 204, session=session, body=None
        )
    
    @ApiVersionAttribute(2, 0)
    def delete_workbook_from_favorites(self, 
                                     session: TableauSession, 
                                     user_id: str, 
                                     workbook_id: str) -> None:
        """
        Deletes the specified workbook from the user's favorites. Available in versions 2.0+
        
        Args:
            session: TableauSession object
            user_id: The ID of the user to remove the favorite from
            workbook_id: The ID of the workbook to remove from the user's favorites
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("user_id", user_id),
            ("workbook_id", workbook_id)
        )
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/favorites/{user_id}/workbooks/{workbook_id}")
        
        self._api_client.api_request(
            uri, "DELETE", 204, session=session, body=None
        )
    
    @ApiVersionAttribute(2, 0)
    def delete_view_from_favorites(self, 
                                 session: TableauSession, 
                                 user_id: str, 
                                 view_id: str) -> None:
        """
        Deletes the specified view from the user's favorites. Available in versions 2.0+
        
        Args:
            session: TableauSession object
            user_id: The ID of the user to remove the favorite from
            view_id: The ID of the view to remove from the user's favorites
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("user_id", user_id),
            ("view_id", view_id)
        )
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/favorites/{user_id}/views/{view_id}")
        
        self._api_client.api_request(
            uri, "DELETE", 204, session=session, body=None
        )
    
    @ApiVersionAttribute(2, 5)
    def get_favorites_for_user(self, 
                             session: TableauSession, 
                             user_id: str) -> List[FavoriteType]:
        """
        Returns a list of favorite projects, data sources, views and workbooks for a user. Versions 2.5+
        
        Args:
            session: TableauSession object
            user_id: The ID of the user for which you want to get a list favorites
            
        Returns:
            List[FavoriteType]: List of user's favorites
            
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
        uri = self._api_client.build_uri(f"sites/{session.site_id}/favorites/{user_id}")
        
        response_content = self._api_client.api_request(
            uri, "GET", 200, session=session, body=None
        )
        
        # Parse response
        favorite_list = self._api_client.get_response_as_object(response_content, FavoriteListType)
        return favorite_list.favorite if favorite_list and favorite_list.favorite else []
from typing import TYPE_CHECKING
from tableau_api_client.models.ts_api import ServerInfo
from tableau_api_client.attributes.api_version_attribute import ApiVersionAttribute

if TYPE_CHECKING:
    from tableau_api_client.tableau_api_client import TableauApiClient


class TableauServerInfoClient:
    """Client for Tableau Server information operations"""
    
    def __init__(self, api_client: "TableauApiClient"):
        """
        Initialize server info client
        
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
            ServerInfoType: Object containing server information including version details
            
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
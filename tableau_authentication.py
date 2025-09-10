from typing import Optional
from client_registry import ClientRegistry
from models.ts_api import SiteType, TableauCredentialsType, UserType
from models.tableau_session import TableauSession
from attributes.api_version_attribute import ApiVersionAttribute
from attributes.on_premise_only_attribute import OnPremiseOnlyAttribute
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tableau_api_client import TableauApiClient


@ClientRegistry.register("authentication")
class TableauAuthenticationClient:
    """Client for Tableau Server authentication operations"""
    
    def __init__(self, api_client: "TableauApiClient"):
        """
        Initialize authentication client
        
        Args:
            api_client: Instance of TableauApiClient
        """
        self._api_client = api_client
    
    @ApiVersionAttribute(1, 0)
    def sign_in(self, 
                user_name: str, 
                password: str, 
                site_content_url: str = "", 
                user_id_to_impersonate: Optional[str] = None) -> TableauSession:
        """
        Signs into Tableau Server. Available in all versions.
        
        Args:
            user_name: The name of the user
            password: The password of the user
            site_content_url: The ContentUrl of the site to log in to. Default: ''
            user_id_to_impersonate: The id of the user to impersonate (Optional)
            
        Returns:
            TableauSession: Session object containing authentication details
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("user_name", user_name), 
            ("password", password)
        )

        # Create user model for impersonation if provided
        user_model = None
        if user_id_to_impersonate:
            user_model = UserType()
            user_model.id = user_id_to_impersonate
        
        # Create credentials object
        credentials = TableauCredentialsType()
        credentials.name = user_name
        credentials.password = password
        credentials.site = SiteType()
        credentials.site.content_url = site_content_url
        credentials.user = user_model
        
        # Build URI and make request
        uri = self._api_client._build_uri("auth/signin")
        request_body = self._api_client._get_object_as_request_content(credentials)
        
        response_content = self._api_client._api_request(
            uri, "POST", 200, session=None, body=request_body
        )
        
        # Parse response
        credentials_response = self._api_client._get_response_as_object(response_content, TableauCredentialsType)
        
        # Set the username in the response (it's not returned by the API)
        if credentials_response and credentials_response.user:
            credentials_response.user.name = user_name
            
        return TableauSession(credentials_response)
    
    @ApiVersionAttribute(3, 7)
    def sign_in_with_pat(self, 
                         token_name: str, 
                         token: str, 
                         site_content_url: str = "", 
                         user_id_to_impersonate: Optional[str] = None) -> TableauSession:
        """
        Signs into Tableau Server with a personal access token. Versions 3.7+
        
        Args:
            token_name: The name of the access token
            token: The token itself
            site_content_url: The ContentUrl of the site to log in to. Default: ''
            user_id_to_impersonate: The LUID of the Tableau User to impersonate. Leave None to skip impersonation
            
        Returns:
            TableauSession: Session object containing authentication details
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("token_name", token_name), 
            ("token", token)
        )
        
        # Create user model for impersonation if provided
        user_model = None
        if user_id_to_impersonate:
            user_model = UserType()
            user_model.id = user_id_to_impersonate
        
        # Create credentials object
        credentials = TableauCredentialsType()
        credentials.personal_access_token_name = token_name
        credentials.personal_access_token_secret = token
        credentials.site = SiteType()
        credentials.site.content_url = site_content_url
        credentials.user = user_model
        
        # Build URI and make request
        uri = self._api_client._build_uri("auth/signin")
        request_body = self._api_client._get_object_as_request_content(credentials)
        
        response_content = self._api_client._api_request(
            uri, "POST", 200, session=None, body=request_body
        )
        
        # Parse response
        credentials_response = self._api_client._get_response_as_object(response_content, TableauCredentialsType)
        
        # Set the token name as username (it's not returned by the API)
        if credentials_response and credentials_response.user:
            credentials_response.user.name = token_name
            
        return TableauSession(credentials_response)
    
    @ApiVersionAttribute(1, 0)
    def sign_out(self, session: TableauSession) -> None:
        """
        Signs out of Tableau Server. Available in all versions.
        
        Args:
            session: The TableauSession to sign out
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If session is None
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(("session", session))
        
        # Build URI and make request
        uri = self._api_client._build_uri("auth/signout")
        
        self._api_client._api_request(
            uri, "POST", 204, session=session, body=None
        )
    
    @ApiVersionAttribute(2, 6)
    @OnPremiseOnlyAttribute()
    def switch_site(self, 
                    session: TableauSession, 
                    new_site_content_url: str) -> TableauSession:
        """
        Changes sites using specified Session. Not available in Tableau Online. Versions 2.6+
        
        Args:
            session: Current TableauSession
            new_site_content_url: Content URL of the site to switch to
            
        Returns:
            TableauSession: New session object for the switched site
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            TableauOnlineNotSupportedException: If used with Tableau Online
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(("session", session))
        
        # Create site object
        site = SiteType()
        site.content_url = new_site_content_url
        
        # Build URI and make request
        uri = self._api_client._build_uri("auth/switchsite")
        request_body = self._api_client._get_object_as_request_content(site)
        
        response_content = self._api_client._api_request(
            uri, "POST", 200, session=session, body=request_body
        )
        
        # Parse response
        credentials_response = self._api_client._get_response_as_object(response_content, TableauCredentialsType)
        
        # Preserve the original username
        if credentials_response and credentials_response.user:
            credentials_response.user.name = session.user_name
            
        return TableauSession(credentials_response)
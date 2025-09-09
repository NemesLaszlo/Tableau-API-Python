from dataclasses import dataclass
from typing import Optional
from .ts_api import TableauCredentialsType


@dataclass
class TableauSession:
    """
    Represents a Tableau session with user credentials and authentication information. 
    This class stores session information after successful authentication with Tableau Server.
    """
    
    user_id: Optional[str] = None
    user_name: Optional[str] = None
    token: Optional[str] = None
    site_id: Optional[str] = None
    
    def __init__(self, credentials: Optional[TableauCredentialsType] = None):
        """
        Initialize a TableauSession.
        
        Args:
            credentials: TableauCredentialsType object containing authentication details.
                        If provided, extracts session information from the credentials.
        """
        if credentials is not None:
            self.user_id = credentials.user.id if credentials.user else None
            self.user_name = credentials.name
            self.token = credentials.token
            self.site_id = credentials.site.id if credentials.site else None
        else:
            self.user_id = None
            self.user_name = None
            self.token = None
            self.site_id = None
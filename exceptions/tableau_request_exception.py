from typing import Optional
from http import HTTPStatus


class TableauRequestException(Exception):
    """
    Exception raised when a Tableau Server request fails.
    """
    
    def __init__(self, request_url: str, status_code: HTTPStatus, details: Optional[dict] = None):
        """
        Initialize the exception.
        
        Args:
            request_url: The URL of the failed request
            status_code: HTTP status code of the response
            details: Optional dictionary containing error details with keys: code, summary, detail
        """
        self.request_url = request_url
        self.status_code = status_code
        self.details = details
        super().__init__(self.message)
    
    @property
    def has_details(self) -> bool:
        """Check if error details are available."""
        return self.details is not None
    
    @property
    def message(self) -> str:
        """Generate the error message."""
        ret = f"A Tableau Server exception occured. HTTP Status Code: {self.status_code.value}"
        
        if self.has_details:
            code = self.details.get('code', 'Unknown')
            summary = self.details.get('summary', 'No summary')
            detail = self.details.get('detail', 'No details')
            ret += f", Internal Code: {code}, Summary: '{summary}', Detailed info: '{detail}'"
        
        return ret
    
    def __str__(self) -> str:
        return self.message
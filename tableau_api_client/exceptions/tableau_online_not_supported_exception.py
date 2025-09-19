class TableauOnlineNotSupportedException(Exception):
    """
    Exception raised when trying to use an endpoint that is not supported in Tableau Online.
    """
    
    def __init__(self, endpoint_name: str):
        """
        Initialize the exception.
        
        Args:
            endpoint_name: Name of the endpoint that is not supported in Tableau Online
        """
        self.endpoint_name = endpoint_name
        super().__init__(self.message)
    
    @property
    def message(self) -> str:
        """Generate the error message."""
        return f"Endpoint '{self.endpoint_name}' not available in Tableau Online"
    
    def __str__(self) -> str:
        return self.message
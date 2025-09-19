from typing import Tuple


class TableauApiVersionException(Exception):
    """
    Exception raised when the current Tableau API version is too low for a specific endpoint.
    """
    
    def __init__(self, endpoint_name: str, version_required: Tuple[int, int], current_version: Tuple[int, int]):
        """
        Initialize the exception.
        
        Args:
            endpoint_name: Name of the endpoint that requires a higher version
            version_required: Required version as a tuple (major, minor)
            current_version: Current API version as a tuple (major, minor)
        """
        self.endpoint_name = endpoint_name
        self.version_required = version_required
        self.current_version = current_version
        super().__init__(self.message)
    
    @property
    def message(self) -> str:
        """Generate the error message."""
        current_version_str = f"{self.current_version[0]}.{self.current_version[1]}"
        required_version_str = f"{self.version_required[0]}.{self.version_required[1]}"
        
        return (f"Current Tableau Api version ({current_version_str}) is too low for endpoint: "
                f"'{self.endpoint_name}'. Version required: {required_version_str}")
    
    def __str__(self) -> str:
        return self.message
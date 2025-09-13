import functools
from typing import Callable


class ApiVersionAttribute:
    """
    Decorator class that specifies the minimum API version required for an endpoint.
    """
    
    def __init__(self, major: int, minor: int = 0):
        """
        Initialize the API version attribute.
        
        Args:
            major: Major version number
            minor: Minor version number (default: 0)
        """
        self.min_version = (major, minor)
    
    def __call__(self, func: Callable) -> Callable:

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        
        # Store the minimum version on the function for later use
        wrapper.min_api_version = self.min_version
        
        return wrapper
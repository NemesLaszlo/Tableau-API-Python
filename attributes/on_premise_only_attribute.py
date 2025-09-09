import functools
from typing import Callable


class OnPremiseOnlyAttribute:
    """
    Decorator class that marks a method as only available for on-premise Tableau installations.
    """
    
    def __call__(self, func: Callable) -> Callable:

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        
        # Mark the function as on-premise only
        wrapper._on_premise_only = True
        
        return wrapper
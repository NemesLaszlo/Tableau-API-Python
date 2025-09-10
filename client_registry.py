class ClientRegistry:
    """Registry for Tableau client classes"""
    _clients = {}
    
    @classmethod
    def register(cls, client_type: str):
        """Decorator to register a client class"""
        def decorator(client_class):
            cls._clients[client_type] = client_class
            return client_class
        return decorator
    
    @classmethod
    def get_clients(cls):
        return cls._clients.copy()
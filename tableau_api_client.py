import logging
import re
import urllib.parse
from datetime import timedelta
from typing import Dict, List, Optional, Tuple, Type, TypeVar, Any, Union
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from io import BytesIO
import inspect
from dataclasses import fields, is_dataclass
from xsdata.formats.dataclass.context import XmlContext
from xsdata.formats.dataclass.parsers import XmlParser
from xsdata.formats.dataclass.serializers import XmlSerializer
from xsdata.formats.dataclass.serializers.config import SerializerConfig

from client_registry import ClientRegistry
from models.ts_api import TsResponse, TsRequest
from models.tableau_session import TableauSession
from exceptions.tableau_api_version_exception import TableauApiVersionException
from exceptions.tableau_online_not_supported_exception import TableauOnlineNotSupportedException
from exceptions.tableau_request_exception import TableauRequestException

T = TypeVar('T')
T2 = TypeVar('T2')


class TableauApiClient:
    """
    Python client for interacting with Tableau Server REST API.
    """
    
    _DEFAULT_TIMEOUT = timedelta(minutes=5)
    _MIN_VERSION = (2, 0)
    _MAX_VERSION = (3, 23)
    _MIN_VERSION_FOR_ENDPOINTS: Dict[str, Tuple[int, int]] = {}
    _ON_PREMISE_ONLY_ENDPOINTS: List[str] = []

    _TABLEAU_ONLINE_REGEX = re.compile(
        r"(?:\.online)?\.tableau\.com$",  # matches any Tableau Online/Cloud URL ending with ".online.tableau.com" or "tableau.com"
        re.IGNORECASE
    )
    
    @classmethod
    def _initialize_endpoint_metadata(cls):
        """Initialize endpoint metadata using the client registry."""
        if cls._MIN_VERSION_FOR_ENDPOINTS:
            return  # Already initialized
          
        # Inspect methods directly on the main class
        for name, method in inspect.getmembers(cls, predicate=inspect.isfunction):
            if hasattr(method, '_min_api_version'):
                cls._MIN_VERSION_FOR_ENDPOINTS[name] = method._min_api_version
            if hasattr(method, '_on_premise_only'):
                cls._ON_PREMISE_ONLY_ENDPOINTS.append(name)
        
        # Inspect methods on all registered client classes
        for client_type, client_class in ClientRegistry.get_clients().items():
            for name, method in inspect.getmembers(client_class, predicate=inspect.isfunction):
                if name.startswith('_'):
                    continue
                    
                full_name = f"{client_type}.{name}" 
                if hasattr(method, '_min_api_version'):
                    cls._MIN_VERSION_FOR_ENDPOINTS[full_name] = method._min_api_version
                if hasattr(method, '_on_premise_only'):
                    cls._ON_PREMISE_ONLY_ENDPOINTS.append(full_name)

    def _initialize_client_instances(self):
        """Initialize all registered client instances as attributes."""
        for client_type, client_class in ClientRegistry.get_clients().items():
            try:
                client_instance = client_class(self)
                setattr(self, client_type, client_instance)
            except Exception as e:
                if self.log:
                    self.log.warning(f"Could not initialize {client_type} client: {e}")
    
    def __init__(self, 
                 tableau_base_uri: str, 
                 logger: Optional[logging.Logger] = None,
                 api_version: str = "2.0",
                 timeout: Optional[timedelta] = None,
                 ignore_ssl_errors: bool = False):
        """
        Initialize the Tableau API client.
        
        Args:
            tableau_base_uri: The base URL of the Tableau Server (e.g., https://my-tableau-server.com)
            logger: Logger instance for debugging
            api_version: Tableau API version to use
            timeout: Request timeout (default: 5 minutes)
            ignore_ssl_errors: Whether to ignore SSL certificate errors
        """
        if not tableau_base_uri:
            raise ValueError("Tableau base Uri is required (eg. http://mytableauserver.com)")
        
        # Parse base URL
        if isinstance(tableau_base_uri, str):
            if not tableau_base_uri.startswith(('http://', 'https://')):
                tableau_base_uri = f'https://{tableau_base_uri}'
            
            from urllib.parse import urlparse
            parsed = urlparse(tableau_base_uri)
            self.base_url = parsed
        else:
            raise ValueError("tableau_base_uri must be a string")
            
        self.log = logger or logging.getLogger(__name__)
        self.ignore_ssl_errors = ignore_ssl_errors
        
        # Parse and validate API version
        try:
            version_parts = api_version.split('.')
            major = int(version_parts[0])
            minor = int(version_parts[1]) if len(version_parts) > 1 else 0
            version_tuple = (major, minor)
            
            if version_tuple < self._MIN_VERSION:
                raise ValueError(f"Tableau API version must be greater than {self._MIN_VERSION[0]}.{self._MIN_VERSION[1]}")
            
            if major <= self._MAX_VERSION[0] and minor <= self._MAX_VERSION[1]:
                self.api_version = version_tuple
            else:
                if self.log:
                    self.log.warning(f"Initialized Tableau Client of version: {self._MAX_VERSION[0]}.{self._MAX_VERSION[1]} "
                                   f"instead of version {major}.{minor} due to library implementation limit")
                self.api_version = self._MAX_VERSION
                
        except (ValueError, IndexError):
            raise ValueError(f"'api_version' parameter cannot be converted to Version object")
        
        if timeout is None:
            self.timeout = self._DEFAULT_TIMEOUT
        elif isinstance(timeout, (int, float)):
            self.timeout = timedelta(seconds=timeout)
        elif isinstance(timeout, timedelta):
            self.timeout = timeout
        else:
            raise TypeError("timeout must be int, float, or timedelta")
        
        # Initialize client instances using registry
        self._initialize_client_instances()
        
        # Initialize endpoint metadata
        self._initialize_endpoint_metadata()
        
        # Initialize XML context and parsers
        self.xml_context = XmlContext()
        self.xml_parser = XmlParser(context=self.xml_context)
        self.xml_serializer = XmlSerializer(
            context=self.xml_context,
            config=SerializerConfig(pretty_print=False)
        )
        
        if self.log:
            self.log.debug(f"Created TableauApiClient for tableau base url: '{tableau_base_uri}', "
                          f"api version: {api_version}")
    

    def _check_null_parameters(self, *args):
        """Check null parameters"""
        for name, value in args:
            if value is None or (isinstance(value, str) and not value.strip()):
                raise ValueError(f"Argument {name} cannot be null")
    
    def _check_empty_arrays(self, *args):
        """Check empty arrays"""
        for name, array in args:
            if hasattr(array, '__len__') and len(array) == 0:
                raise ValueError(f"Argument array '{name}' cannot be empty")
    
    def _check_parameters_between(self, *args):
        """Check parameters between ranges"""
        for name, value, min_val, max_val in args:
            if not (min_val <= value <= max_val):
                raise ValueError(f"Argument '{name}' must be between {min_val} and {max_val}")
            
    def _is_online(self) -> bool:
        """
        Detect if this Tableau Server is Tableau Online.
        Returns True if the base URL matches Tableau Online pattern.
        """
        hostname = getattr(self.base_url, 'hostname', '')
        if not hostname:
            return False
        return bool(self._TABLEAU_ONLINE_REGEX.search(hostname))
    
    def _check_endpoint_availability(self, method_name: str = None):
        """Check if the endpoint is available for the current API version"""
        if not method_name:
            # Get the calling method name
            frame = inspect.currentframe().f_back
            method_name = frame.f_code.co_name
        
        # Determine the client namespace by inspecting the call stack
        submodule = self._detect_client_namespace()
        
        # Build the key for namespaced endpoints
        key = f"{submodule}.{method_name}" if submodule else method_name

        # Check minimum version
        if key in self._MIN_VERSION_FOR_ENDPOINTS:
            required_version = self._MIN_VERSION_FOR_ENDPOINTS[key]
            if required_version > self.api_version:
                raise TableauApiVersionException(key, required_version, self.api_version)

        # Check on-premise only endpoints
        if key in self._ON_PREMISE_ONLY_ENDPOINTS:
            if self._is_online():
                raise TableauOnlineNotSupportedException(key)

    def _detect_client_namespace(self):
        """
        Detect which client namespace the current method call is coming from
        by inspecting the call stack and matching against registered clients.
        """
        try:
            # Walk up the call stack to find the client class
            frame = inspect.currentframe()
            while frame:
                frame = frame.f_back
                if frame is None:
                    break
                
                # Get the 'self' object from the frame's local variables
                frame_locals = frame.f_locals
                if 'self' in frame_locals:
                    calling_self = frame_locals['self']
                    calling_class = calling_self.__class__
                    
                    # Skip if it's the TableauApiClient itself
                    if calling_class == TableauApiClient:
                        continue
                    
                    # Check if this class is registered in our client registry
                    for client_type, client_class in ClientRegistry.get_clients().items():
                        if calling_class == client_class:
                            return client_type
            
            return None  # No client namespace detected (direct call on main class)
            
        except Exception as e:
            if self.log:
                self.log.warning(f"Could not detect client namespace: {e}")
            return None
    
    def _build_client(self, auth_token: str = None) -> requests.Session:
        """Build a requests session"""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set headers
        session.headers.clear()
        session.headers['Accept'] = 'application/xml'
        
        if auth_token:
            session.headers['X-Tableau-Auth'] = auth_token
        
        # SSL verification
        session.verify = not self.ignore_ssl_errors
        
        return session
    
    def _build_uri(self, relative_path: str, *url_params) -> str:
        """Build the full API URL"""
        # Remove leading slash if present
        if relative_path.startswith('/'):
            relative_path = relative_path[1:]
        
        port = self.base_url.port or (443 if self.base_url.scheme == 'https' else 80)
        url = f"{self.base_url.scheme}://{self.base_url.hostname}:{port}/api/{self.api_version[0]}.{self.api_version[1]}/{relative_path}"
        
        if url_params:
            query_parts = []
            for key, value in url_params:
                if key in ['filter', 'sort']:
                    query_parts.append(f"{key}={value}")
                else:
                    query_parts.append(f"{key}={urllib.parse.quote_plus(str(value))}")
            url += f"?{'&'.join(query_parts)}"
        
        return url
    
    def _build_exception(self, response: requests.Response) -> TableauRequestException:
        """Build exception from failed HTTP response"""
        error_details = None
        
        try:
            error_details = self._get_response_as_object(response.text, type(None))  # ErrorType equivalent
        except Exception as e:
            if self.log:
                self.log.warning(f"Failed to read Tableau Server error details: {e}")
        
        return TableauRequestException(response.url, response.status_code, error_details)
    
    def _api_request(self, full_uri: str, method: str, expected_status_code: int, session: Optional[TableauSession] = None, body=None) -> str:
        """Make API request to Tableau Server"""
        try:
            if self.log:
                self.log.info(f"Sending request to Tableau Server. Method: {method}, "
                             f"Url: '{full_uri}'. Expected response code: {expected_status_code}")
            
            with self._build_client(session.token if session else None) as http_session:
                # Prepare request parameters
                request_params = {
                    'timeout': self.timeout.total_seconds(),
                }
                
                # Handle different body types
                if isinstance(body, dict) and 'files' in body:
                    # Multipart file upload
                    request_params['files'] = body['files']
                elif body:
                    # Regular request body
                    request_params['data'] = body
                    if not http_session.headers.get('Content-Type'):
                        http_session.headers['Content-Type'] = 'application/xml'
                
                # Make request
                response = http_session.request(method, full_uri, **request_params)
                
                if response.status_code != expected_status_code:
                    raise self._build_exception(response)
                
                if self.log:
                    self.log.info("Request successful")
                return response.text
                
        except TableauRequestException as e:
            if self.log:
                self.log.error(f"Tableau Server returned an error at request. "
                              f"{'Tableau Server server code: ' + getattr(e.details, 'code', 'Unknown') if e.has_details else 'No details available'}")
            raise
        except requests.RequestException as e:
            if self.log:
                self.log.error(f"A network error occured while communicating with Tableau Server. Internal error message: {e}")
            raise
        except Exception as e:
            if self.log:
                self.log.error(f"An unhandled error occured while communicating with Tableau Server. Internal error message: {e}")
            raise
    
    def _read_response_string(self, content: str) -> str:
        """Process response string, replacing old schema references and handling namespaces"""
        # Replace old schema references
        cleaned_content = content.replace("http://tableausoftware.com/api", "http://tableau.com/api")
                
        # Remove the xmlns namespace declarations
        cleaned_content = re.sub(r'\s+xmlns[^=]*="[^"]*"', '', cleaned_content)
        
        # Remove namespace prefixes from element names (e.g., ts:credentials -> credentials)
        cleaned_content = re.sub(r'<([^:\s>]+:)?([^>\s]+)', r'<\2', cleaned_content)
        cleaned_content = re.sub(r'</([^:\s>]+:)?([^>\s]+)', r'</\2', cleaned_content)
        
        return cleaned_content
    
    def _get_response_as_object(self, content: str, target_class: Type[T]) -> Optional[T]:
        """
        Extract single object of specified type from response.
        Search through all TsResponse fields to find target_class instance
        """
        try:
            # Clean the XML content
            cleaned_content = self._read_response_string(content)
            
            # Parse XML using xsdata parser - use BytesIO instead of StringIO
            with BytesIO(cleaned_content.encode('utf-8')) as reader:
                ts_response = self.xml_parser.parse(reader, TsResponse)
            
            # Search through all fields of TsResponse to find an instance of target_class
            if not is_dataclass(ts_response):
                return None
                
            for field in fields(ts_response):
                field_value = getattr(ts_response, field.name, None)
                
                if field_value is None:
                    continue
                
                # Direct type match - the field itself is the target type
                if isinstance(field_value, target_class):
                    return field_value
                
                # Check if it's a collection/list that might contain the target type
                if hasattr(field_value, '__iter__') and not isinstance(field_value, (str, bytes)):
                    try:
                        # Handle list-like objects
                        if hasattr(field_value, '__len__'):
                            for item in field_value:
                                if isinstance(item, target_class):
                                    return item
                        # Handle objects with items attribute (like list wrappers)
                        elif hasattr(field_value, 'items') and field_value.items is not None:
                            if hasattr(field_value.items, '__iter__'):
                                for item in field_value.items:
                                    if isinstance(item, target_class):
                                        return item
                            elif isinstance(field_value.items, target_class):
                                return field_value.items
                    except (TypeError, AttributeError):
                        continue
            
            return None
            
        except Exception as e:
            if self.log:
                self.log.error(f"Failed to parse XML response: {e}")
            raise
    
    def _get_response_as_objects(self, content: str, 
                                target_class1: Type[T], 
                                target_class2: Type[T2]) -> Tuple[Optional[T], Optional[T2]]:
        """
        Extract two objects of different types from response.
        """
        try:
            # Clean the XML content
            cleaned_content = self._read_response_string(content)
            
            # Parse XML using xsdata parser - use BytesIO instead of StringIO
            with BytesIO(cleaned_content.encode('utf-8')) as reader:
                ts_response = self.xml_parser.parse(reader, TsResponse)
            
            result1 = None
            result2 = None
            
            # Search through all fields of TsResponse to find instances of both target classes
            if not is_dataclass(ts_response):
                return None, None
                
            for field in fields(ts_response):
                field_value = getattr(ts_response, field.name, None)
                
                if field_value is None:
                    continue
                
                # Direct matches
                if result1 is None and isinstance(field_value, target_class1):
                    result1 = field_value
                elif result2 is None and isinstance(field_value, target_class2):
                    result2 = field_value
                
                # Check collections
                if hasattr(field_value, '__iter__') and not isinstance(field_value, (str, bytes)):
                    try:
                        # Handle list-like objects
                        if hasattr(field_value, '__len__'):
                            for item in field_value:
                                if result1 is None and isinstance(item, target_class1):
                                    result1 = item
                                elif result2 is None and isinstance(item, target_class2):
                                    result2 = item
                        # Handle objects with items attribute
                        elif hasattr(field_value, 'items') and field_value.items is not None:
                            if hasattr(field_value.items, '__iter__'):
                                for item in field_value.items:
                                    if result1 is None and isinstance(item, target_class1):
                                        result1 = item
                                    elif result2 is None and isinstance(item, target_class2):
                                        result2 = item
                    except (TypeError, AttributeError):
                        continue
                
                # Break if we found both
                if result1 is not None and result2 is not None:
                    break
            
            return result1, result2
            
        except Exception as e:
            if self.log:
                self.log.error(f"Failed to parse XML response: {e}")
            return None, None
    
    def _get_object_as_request_content(self, obj: Any) -> str:
        """
        Convert object to XML request content.
        Must set the appropriate field in TsRequest based on object type
        """
        try:
            # Create TsRequest wrapper
            ts_request = TsRequest()
            
            obj_set = False
            
            # Try to match object type to TsRequest fields by iterating through TsRequest fields
            for field in fields(ts_request):
                field_type = field.type
                
                # Handle Optional[T] types - extract the actual type
                if hasattr(field_type, '__origin__') and field_type.__origin__ is Union:
                    # Get non-None type from Optional
                    non_none_args = [arg for arg in field_type.__args__ if arg is not type(None)]
                    if non_none_args:
                        field_type = non_none_args[0]
                
                # Handle List[T] types
                if hasattr(field_type, '__origin__') and field_type.__origin__ is list:
                    if hasattr(field_type, '__args__') and field_type.__args__:
                        field_type = field_type.__args__[0]
                
                # Check if object matches this field's type
                try:
                    if isinstance(obj, field_type) or type(obj).__name__ == field_type.__name__:
                        setattr(ts_request, field.name, obj)
                        obj_set = True
                        break
                except (TypeError, AttributeError):
                    continue
            
            if not obj_set:
                # Fallback: try by object type name matching field names
                obj_type_name = type(obj).__name__.lower()
                
                for field in fields(ts_request):
                    field_name = field.name.lower()
                    # Remove common suffixes and compare
                    obj_name_clean = obj_type_name.replace('type', '').replace('_', '')
                    field_name_clean = field_name.replace('_', '')
                    
                    if (obj_name_clean in field_name_clean or 
                        field_name_clean in obj_name_clean or
                        field_name == obj_name_clean):
                        setattr(ts_request, field.name, obj)
                        obj_set = True
                        break
            
            if not obj_set:
                raise ValueError(f"Cannot map object type {type(obj).__name__} to any TsRequest field")
            
            # Serialize to XML using xsdata serializer
            xml_content = self.xml_serializer.render(ts_request)
            
            # Remove first (XML schema) tag for compatibility with old versions of API
            if xml_content.startswith('<?xml'):
                xml_content = xml_content[xml_content.find('?>') + 2:].strip()
            
            # Remove XML definitions from tsRequest
            xml_content = re.sub(r'(?<=<tsRequest )[^>]+', '', xml_content).strip()
            
            # Handle self-closing tags
            match = re.search(r'<([^>]*)>$', xml_content)
            if match and not match.group(1).startswith('/'):
                xml_content = xml_content[:-1] + '/>'
            
            return xml_content
            
        except Exception as e:
            if self.log:
                self.log.error(f"Failed to serialize object to XML: {e}")
            raise
    
    def _prepare_file_upload_content(self, xml_content: str, 
                                   file_stream: Optional[BytesIO] = None) -> dict:
        """
        Prepare multipart form data for file upload.
        """
        files = {}
        
        # Add XML payload with proper content-disposition
        files['request_payload'] = ('', xml_content, 'application/xml')
        
        # Add file if provided
        if file_stream is not None:
            files['tableau_file'] = ('FILE-NAME', file_stream, 'application/octet-stream')
        
        return {'files': files}
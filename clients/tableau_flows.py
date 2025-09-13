from typing import Optional, List, Tuple, TYPE_CHECKING
from models.tableau_session import TableauSession
from attributes.api_version_attribute import ApiVersionAttribute
from models.ts_api import (
    FlowType, FlowOutputStepType, FlowOutputStepListType, FlowListType,
    PaginationType, ConnectionType, ConnectionListType, FlowRunType,
    FlowRunListType, TaskRunFlowType, TaskType, TaskListType,
    LinkedTaskType, LinkedTaskListType, JobType, LinkedTaskJobType,
    ProjectType, UserType
)

if TYPE_CHECKING:
    from tableau_api_client import TableauApiClient


class TableauFlowsClient:
    """Client for Tableau Server flows operations"""
    
    def __init__(self, api_client: "TableauApiClient"):
        """
        Initialize flows client
        
        Args:
            api_client: Instance of TableauApiClient
        """
        self._api_client = api_client
    
    @ApiVersionAttribute(3, 3)
    def query_flow(self, session: TableauSession, flow_id: str) -> Tuple[List[FlowOutputStepType], FlowType]:
        """
        Returns information about the specified flow, including information about the project, owner, and output steps. Versions 3.3+
        
        Args:
            session: TableauSession object
            flow_id: The ID of the flow to return information about
            
        Returns:
            Tuple containing:
                - List of flow output steps
                - Flow object
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session), 
            ("flow_id", flow_id)
        )
        
        uri = self._api_client._build_uri(f"sites/{session.site_id}/flows/{flow_id}")
        response_content = self._api_client._api_request(
            uri, "GET", 200, session=session
        )
        
        # Parse response to get both objects
        output_steps_obj, flow_obj = self._api_client._get_response_as_objects(
            response_content, FlowOutputStepListType, FlowType
        )
        
        output_steps = output_steps_obj.flow_output_step if output_steps_obj else []
        return output_steps, flow_obj
    
    @ApiVersionAttribute(3, 3)
    def query_flows_for_site(self, 
                            session: TableauSession,
                            page_size: int = 100,
                            page_number: int = 1,
                            filter_expression: Optional[str] = None,
                            sort_expression: Optional[str] = None) -> Tuple[PaginationType, List[FlowType]]:
        """
        Returns the flows on a site. If the user is not an administrator, the method returns just the flows that the user has permissions to view. Versions 3.3+
        
        Args:
            session: TableauSession object
            page_size: The number of items to return in one response. The minimum is 1. The maximum is 1000
            page_number: The offset for paging. The default is 1
            filter_expression: An expression that lets you specify a subset of flows to return
            sort_expression: An expression that lets you specify the order in which flow information is returned
            
        Returns:
            Tuple containing:
                - Pagination information
                - List of flows
                
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing or out of range
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(("session", session))
        self._api_client._check_parameters_between(
            ("page_size", page_size, 1, 1000),
            ("page_number", page_number, 1, float('inf'))
        )
        
        # Build query parameters
        url_params = [
            ("pageSize", str(page_size)),
            ("pageNumber", str(page_number))
        ]
        
        if filter_expression:
            url_params.append(("filter", filter_expression))
        if sort_expression:
            url_params.append(("sort", sort_expression))
        
        uri = self._api_client._build_uri(f"sites/{session.site_id}/flows", *url_params)
        response_content = self._api_client._api_request(
            uri, "GET", 200, session=session
        )
        
        # Parse response to get both objects
        pagination_obj, flow_list_obj = self._api_client._get_response_as_objects(
            response_content, PaginationType, FlowListType
        )
        
        flows = flow_list_obj.flow if flow_list_obj else []
        return pagination_obj, flows
    
    @ApiVersionAttribute(3, 3)
    def query_flows_for_user(self,
                            session: TableauSession,
                            user_id: str,
                            is_owner: bool = False,
                            page_size: int = 100,
                            page_number: int = 1) -> Tuple[PaginationType, List[FlowType]]:
        """
        Returns the flows that the specified user owns in addition to those that the user has Read (view) permissions for. Versions 3.3+
        
        Args:
            session: TableauSession object
            user_id: The ID of the user to get flows for
            is_owner: true to return only flows that the specified user owns, or false to return all flows that the specified user has at least read access to
            page_size: The number of items to return in one response. The minimum is 1. The maximum is 1000
            page_number: The offset for paging. The default is 1
            
        Returns:
            Tuple containing:
                - Pagination information
                - List of flows
                
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session),
            ("user_id", user_id)
        )
        
        uri = self._api_client._build_uri(
            f"sites/{session.site_id}/users/{user_id}/flows",
            ("ownedBy", str(is_owner).lower()),
            ("pageSize", str(page_size)),
            ("pageNumber", str(page_number))
        )
        
        response_content = self._api_client._api_request(
            uri, "GET", 200, session=session
        )
        
        # Parse response to get both objects
        pagination_obj, flow_list_obj = self._api_client._get_response_as_objects(
            response_content, PaginationType, FlowListType
        )
        
        flows = flow_list_obj.flow if flow_list_obj else []
        return pagination_obj, flows
    
    @ApiVersionAttribute(3, 3)
    def query_flow_connections(self, session: TableauSession, flow_id: str) -> List[ConnectionType]:
        """
        Returns a list of data connections for the specific flow. Versions 3.3+
        
        Args:
            session: TableauSession object
            flow_id: The ID of the flow to return connection information about
            
        Returns:
            List of connection objects
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session),
            ("flow_id", flow_id)
        )
        
        uri = self._api_client._build_uri(f"sites/{session.site_id}/flows/{flow_id}/connections")
        response_content = self._api_client._api_request(
            uri, "GET", 200, session=session
        )
        
        # Parse response
        connection_list_obj = self._api_client._get_response_as_object(response_content, ConnectionListType)
        return connection_list_obj.connection if connection_list_obj else []
    
    @ApiVersionAttribute(3, 3)
    def update_flow(self,
                   session: TableauSession,
                   flow_id: str,
                   new_project_id: Optional[str] = None,
                   new_owner_id: Optional[str] = None) -> FlowType:
        """
        Updates the owner, project, of the specified flow. Versions 3.3+
        
        Args:
            session: TableauSession object
            flow_id: The ID of the flow to update
            new_project_id: The ID of a project to add the flow to
            new_owner_id: The ID of a user to assign the flow to as owner
            
        Returns:
            Updated flow object
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session),
            ("flow_id", flow_id)
        )
        
        # Create flow object for update
        flow_update = FlowType()
        if new_project_id:
            flow_update.project = ProjectType()
            flow_update.project.id = new_project_id
        if new_owner_id:
            flow_update.owner = UserType()
            flow_update.owner.id = new_owner_id
        
        uri = self._api_client._build_uri(f"sites/{session.site_id}/flows/{flow_id}")
        request_body = self._api_client._get_object_as_request_content(flow_update)
        
        response_content = self._api_client._api_request(
            uri, "PUT", 200, session=session, body=request_body
        )
        
        # Parse response
        return self._api_client._get_response_as_object(response_content, FlowType)
    
    @ApiVersionAttribute(3, 3)
    def update_flow_connection(self,
                              session: TableauSession,
                              flow_id: str,
                              connection_id: str,
                              server_address: Optional[str] = None,
                              server_port: Optional[str] = None,
                              user_name: Optional[str] = None,
                              password: Optional[str] = None,
                              embed_password: Optional[bool] = None) -> ConnectionType:
        """
        Updates the server address, port, username, or password for the specified flow connection. The connection can be an input or an output connection. Versions 3.3+
        
        Args:
            session: TableauSession object
            flow_id: The ID of the flow to update
            connection_id: The ID of the connection to update
            server_address: The new server for the connection
            server_port: The new port for the connection
            user_name: The new username for the connection
            password: The new password for the connection
            embed_password: true to embed the password; otherwise, false
            
        Returns:
            Updated connection object
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session),
            ("flow_id", flow_id),
            ("connection_id", connection_id)
        )
        
        # Create connection object for update
        connection_update = ConnectionType()
        if server_address is not None:
            connection_update.server_address = server_address
        if server_port is not None:
            connection_update.server_port = server_port
        if user_name is not None:
            connection_update.user_name = user_name
        if password is not None:
            connection_update.password = password
        if embed_password is not None:
            connection_update.embed_password = embed_password
        
        uri = self._api_client._build_uri(f"sites/{session.site_id}/flows/{flow_id}/connections/{connection_id}")
        request_body = self._api_client._get_object_as_request_content(connection_update)
        
        response_content = self._api_client._api_request(
            uri, "PUT", 200, session=session, body=request_body
        )
        
        # Parse response
        return self._api_client._get_response_as_object(response_content, ConnectionType)
    
    @ApiVersionAttribute(3, 10)
    def get_flow_run(self, session: TableauSession, flow_run_id: str) -> FlowRunType:
        """
        Gets a flow run. Versions 3.10+
        
        Args:
            session: TableauSession object
            flow_run_id: The ID of the flow run
            
        Returns:
            Flow run object
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session),
            ("flow_run_id", flow_run_id)
        )
        
        uri = self._api_client._build_uri(f"sites/{session.site_id}/flows/runs/{flow_run_id}")
        response_content = self._api_client._api_request(
            uri, "GET", 200, session=session
        )
        
        # Parse response
        return self._api_client._get_response_as_object(response_content, FlowRunType)
    
    @ApiVersionAttribute(3, 10)
    def get_flow_runs(self, 
                     session: TableauSession,
                     filter_expression: Optional[str] = None) -> List[FlowRunType]:
        """
        Get flow runs. Versions 3.10+
        
        Args:
            session: TableauSession object
            filter_expression: An expression that lets you specify a subset to return. You can filter on predefined fields such as the userId of the user who started the flow run, flowId, progress, startedAt, and completedAt
            
        Returns:
            List of flow run objects
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(("session", session))
        
        url_params = []
        if filter_expression:
            url_params.append(("filter", filter_expression))
        
        uri = self._api_client._build_uri(f"sites/{session.site_id}/flows/runs", *url_params)
        response_content = self._api_client._api_request(
            uri, "GET", 200, session=session
        )
        
        # Parse response
        flow_run_list_obj = self._api_client._get_response_as_object(response_content, FlowRunListType)
        return flow_run_list_obj.flow_runs if flow_run_list_obj else []
    
    @ApiVersionAttribute(3, 3)
    def get_flow_run_task(self, session: TableauSession, task_id: str) -> TaskRunFlowType:
        """
        Returns information about the specified flow run task. This method shows you information about the scheduled task for the flow. Versions 3.3+
        
        Args:
            session: TableauSession object
            task_id: The ID of the scheduled flow run task that you want information about
            
        Returns:
            Task run flow object
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session),
            ("task_id", task_id)
        )
        
        uri = self._api_client._build_uri(f"sites/{session.site_id}/tasks/runFlow/{task_id}")
        response_content = self._api_client._api_request(
            uri, "GET", 200, session=session
        )
        
        # Parse response
        task_obj = self._api_client._get_response_as_object(response_content, TaskType)
        return task_obj.flow_run if task_obj else None
    
    @ApiVersionAttribute(3, 3)
    def get_flow_run_tasks(self, session: TableauSession) -> List[TaskRunFlowType]:
        """
        Returns a list of scheduled flow tasks for the site. Versions 3.3+
        
        Args:
            session: TableauSession object
            
        Returns:
            List of task run flow objects
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(("session", session))
        
        uri = self._api_client._build_uri(f"sites/{session.site_id}/tasks/runFlow")
        response_content = self._api_client._api_request(
            uri, "GET", 200, session=session
        )
        
        # Parse response
        task_list_obj = self._api_client._get_response_as_object(response_content, TaskListType)
        if not task_list_obj or not task_list_obj.task:
            return []
        
        # Filter for TaskRunFlowType items
        flow_tasks = []
        for task in task_list_obj.task:
            if isinstance(task.flow_run, TaskRunFlowType):
                flow_tasks.append(task.flow_run)
        
        return flow_tasks
    
    @ApiVersionAttribute(3, 15)
    def get_linked_task(self, session: TableauSession, linked_task_id: str) -> LinkedTaskType:
        """
        Returns information about a specific linked task. This method shows you information about the scheduled linked task. Versions 3.15+
        
        Args:
            session: TableauSession object
            linked_task_id: The ID of the scheduled linked task that you want information about
            
        Returns:
            Linked task object
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session),
            ("linked_task_id", linked_task_id)
        )
        
        uri = self._api_client._build_uri(f"sites/{session.site_id}/tasks/linked/{linked_task_id}")
        response_content = self._api_client._api_request(
            uri, "GET", 200, session=session
        )
        
        # Parse response
        return self._api_client._get_response_as_object(response_content, LinkedTaskType)
    
    @ApiVersionAttribute(3, 15)
    def get_linked_tasks(self, session: TableauSession) -> List[LinkedTaskType]:
        """
        Returns a list of scheduled linked tasks for a site. Versions 3.15+
        
        Args:
            session: TableauSession object
            
        Returns:
            List of linked task objects
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(("session", session))
        
        uri = self._api_client._build_uri(f"sites/{session.site_id}/tasks/linked")
        response_content = self._api_client._api_request(
            uri, "GET", 200, session=session
        )
        
        # Parse response
        linked_task_list_obj = self._api_client._get_response_as_object(response_content, LinkedTaskListType)
        return linked_task_list_obj.linked_tasks if linked_task_list_obj else []
    
    @ApiVersionAttribute(3, 3)
    def run_flow(self,
                session: TableauSession,
                flow_id: str,
                flow_output_step_ids: Optional[str] = None,
                flow_run_mode: Optional[str] = None) -> JobType:
        """
        Runs the specified flow. Versions 3.3+
        
        Args:
            session: TableauSession object
            flow_id: The ID of the flow to run
            flow_output_step_ids: The ID of the output steps you want to run. This parameter is optional. If you don't specify the output steps, all the output steps in the flow will be included
            flow_run_mode: The mode to use for running this flow, either 'full' or 'incremental'. This parameter is optional. If you don't specify an option the run mode will be full
            
        Returns:
            Job object representing the flow run
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session),
            ("flow_id", flow_id)
        )
        
        url_params = []
        if flow_output_step_ids:
            url_params.append(("flowOutputStepIds", flow_output_step_ids))
        if flow_run_mode:
            url_params.append(("flowRunMode", flow_run_mode))
        
        uri = self._api_client._build_uri(f"sites/{session.site_id}/flows/{flow_id}/run", *url_params)
        
        # Empty request body is required for POST
        request_body = self._api_client._get_object_as_request_content(None)
        
        response_content = self._api_client._api_request(
            uri, "POST", 200, session=session, body=request_body
        )
        
        # Parse response
        return self._api_client._get_response_as_object(response_content, JobType)
    
    @ApiVersionAttribute(3, 3)
    def run_flow_task(self, session: TableauSession, task_id: str) -> JobType:
        """
        Runs the specified flow run task. Versions 3.3+
        This method is unavailable if you don't have Data Management.
        This method will fail and result in an error if your Server Administrator has disabled the RunNow setting for the site.
        
        Args:
            session: TableauSession object
            task_id: The ID of the flow run task that you want to run
            
        Returns:
            Job object representing the task run
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session),
            ("task_id", task_id)
        )
        
        uri = self._api_client._build_uri(f"sites/{session.site_id}/tasks/runFlow/{task_id}/runNow")
        
        # Empty request body is required for POST
        request_body = self._api_client._get_object_as_request_content(None)
        
        response_content = self._api_client._api_request(
            uri, "POST", 200, session=session, body=request_body
        )
        
        # Parse response
        return self._api_client._get_response_as_object(response_content, JobType)
    
    @ApiVersionAttribute(3, 15)
    def run_linked_task(self, session: TableauSession, task_id: str) -> LinkedTaskJobType:
        """
        Runs the specified linked task. Versions 3.15+
        This method runs the specified linked task with all the steps. Depending on the setting that the task was created with, the linked task will stop at a step on failure or continue to the next step.
        
        Args:
            session: TableauSession object
            task_id: The ID of the linked task to run
            
        Returns:
            Linked task job object representing the task run
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session),
            ("task_id", task_id)
        )
        
        uri = self._api_client._build_uri(f"sites/{session.site_id}/tasks/linked/{task_id}/runNow")
        
        # Empty request body is required for POST
        request_body = self._api_client._get_object_as_request_content(None)
        
        response_content = self._api_client._api_request(
            uri, "POST", 200, session=session, body=request_body
        )
        
        # Parse response
        return self._api_client._get_response_as_object(response_content, LinkedTaskJobType)
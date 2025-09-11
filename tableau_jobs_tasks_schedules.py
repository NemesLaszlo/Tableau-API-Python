from typing import Optional, List, Tuple, TYPE_CHECKING
from datetime import timedelta, datetime, time
from client_registry import ClientRegistry
from models.ts_api import (
    JobType, TaskExtractRefreshType, TaskType, TaskRunFlowType, ScheduleType,
    PaginationType, BackgroundJobType, ExtractType, TaskListType,
    BackgroundJobListType, ExtractListType, ScheduleListType,
    ScheduleTypeFrequency, ScheduleTypeType, ScheduleTypeExecutionOrder,
    IntervalType, IntervalTypeMinutes, IntervalTypeHours, IntervalTypeWeekDay,
    FrequencyDetailsType
)
from models.tableau_session import TableauSession
from attributes.api_version_attribute import ApiVersionAttribute
from attributes.on_premise_only_attribute import OnPremiseOnlyAttribute

if TYPE_CHECKING:
    from tableau_api_client import TableauApiClient


@ClientRegistry.register("jobs_tasks_schedules")
class TableauJobsTasksSchedulesClient:
    """Client for Tableau Server jobs, tasks, and schedules operations"""
    
    def __init__(self, api_client: "TableauApiClient"):
        """
        Initialize jobs, tasks, and schedules client
        
        Args:
            api_client: Instance of TableauApiClient
        """
        self._api_client = api_client
    
    @ApiVersionAttribute(3, 1)
    def cancel_job(self, session: TableauSession, job_id: str) -> None:
        """
        Cancels a job specified by job ID. Versions 3.1+
        
        Args:
            session: TableauSession object
            job_id: The ID of the job to cancel
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session), 
            ("job_id", job_id)
        )
        
        uri = self._api_client._build_uri(f"sites/{session.site_id}/jobs/{job_id}")
        
        self._api_client._api_request(
            uri, "PUT", 200, session=session, body=None
        )
    
    @ApiVersionAttribute(2, 0)
    def query_job(self, session: TableauSession, job_id: str) -> JobType:
        """
        Gets the details of a specific job. Versions 2.0+
        
        Args:
            session: TableauSession object
            job_id: The ID of the job to query
            
        Returns:
            JobType: Job details
            
        Raises:
            TableauRequestException: If the request fails
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session), 
            ("job_id", job_id)
        )
        
        uri = self._api_client._build_uri(f"sites/{session.site_id}/jobs/{job_id}")
        
        response_content = self._api_client._api_request(
            uri, "GET", 200, session=session, body=None
        )
        
        return self._api_client._get_response_as_object(response_content, JobType)
    
    @ApiVersionAttribute(3, 1)
    def query_jobs(self, 
                   session: TableauSession,
                   page_size: int = 100, 
                   page_number: int = 1, 
                   filter_expression: Optional[str] = None) -> Tuple[PaginationType, List[BackgroundJobType]]:
        """
        Returns a list of active jobs on the specified site. Versions 3.1+
        
        Args:
            session: TableauSession object
            page_size: The number of items to return in one response (1-1000, default: 100)
            page_number: The offset for paging (default: 1)
            filter_expression: An expression to filter jobs
            
        Returns:
            Tuple containing pagination info and list of background jobs
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If parameters are invalid
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(("session", session))
        self._api_client._check_parameters_between(
            ("page_size", page_size, 1, 1000),
            ("page_number", page_number, 1, float('inf'))
        )
        
        url_params = [
            ("pageSize", str(page_size)),
            ("pageNumber", str(page_number))
        ]
        
        if filter_expression:
            url_params.append(("filter", filter_expression))
        
        uri = self._api_client._build_uri(f"sites/{session.site_id}/jobs", *url_params)
        
        response_content = self._api_client._api_request(
            uri, "GET", 200, session=session, body=None
        )
        
        pagination, job_list = self._api_client._get_response_as_objects(
            response_content, PaginationType, BackgroundJobListType
        )
        
        jobs = job_list.background_job if job_list and job_list.background_job else []
        return pagination, jobs
    
    @ApiVersionAttribute(2, 6)
    def get_extract_refresh_task(self, session: TableauSession, task_id: str) -> TaskExtractRefreshType:
        """
        Returns information about the specified extract refresh task. Versions 2.6+
        
        Args:
            session: TableauSession object
            task_id: The ID of the extract refresh task
            
        Returns:
            TaskExtractRefreshType: Extract refresh task details
            
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
        
        uri = self._api_client._build_uri(f"sites/{session.site_id}/tasks/{task_id}")
        
        response_content = self._api_client._api_request(
            uri, "GET", 200, session=session, body=None
        )
        
        task = self._api_client._get_response_as_object(response_content, TaskType)
        
        if task and task.extract_refresh:
            return task.extract_refresh

        return None
    
    @ApiVersionAttribute(2, 6)
    def get_extract_refresh_tasks(self, session: TableauSession) -> Tuple[List[TaskExtractRefreshType], List[TaskRunFlowType], List[ScheduleType]]:
        """
        Returns a list of extract refresh tasks for the site. Versions 2.6+
        
        Args:
            session: TableauSession object
            
        Returns:
            Tuple containing lists of extract refresh tasks, run flow tasks, and schedules
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If session is None
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(("session", session))
        
        uri = self._api_client._build_uri(f"sites/{session.site_id}/tasks/extractRefreshes")
        
        response_content = self._api_client._api_request(
            uri, "GET", 200, session=session, body=None
        )
        
        task_list = self._api_client._get_response_as_object(response_content, TaskListType)
        
        extract_tasks = []
        flow_tasks = []
        schedules = []
        
        if task_list and task_list.task:
            for task in task_list.task:
                if task.extract_refresh:
                    extract_tasks.append(task.extract_refresh)
                if task.flow_run:
                    flow_tasks.append(task.flow_run)
                if task.schedule:
                    schedules.append(task.schedule)
        
        return extract_tasks, flow_tasks, schedules
    
    @ApiVersionAttribute(2, 6)
    def run_extract_refresh_task(self, session: TableauSession, task_id: str) -> JobType:
        """
        Runs the specified extract refresh task. Versions 2.6+
        
        Args:
            session: TableauSession object
            task_id: The ID of the extract refresh task to run
            
        Returns:
            JobType: Information about the started job
            
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
        
        uri = self._api_client._build_uri(f"sites/{session.site_id}/tasks/extractRefreshes/{task_id}/runNow")
        request_body = self._api_client._get_object_as_request_content(None)
        
        response_content = self._api_client._api_request(
            uri, "POST", 200, session=session, body=request_body
        )
        
        return self._api_client._get_response_as_object(response_content, JobType)
    
    def _create_schedule(self, 
                        session: TableauSession,
                        name: str, 
                        priority: int,
                        frequency: ScheduleTypeFrequency,
                        schedule_type: ScheduleTypeType,
                        execution_order: ScheduleTypeExecutionOrder,
                        start_time: time,
                        end_time: Optional[time] = None,
                        intervals: Optional[List[IntervalType]] = None) -> ScheduleType:
        """
        Internal method to create a schedule with specified parameters.
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session), 
            ("name", name)
        )
        self._api_client._check_parameters_between(
            ("priority", priority, 1, 100)
        )
        
        # Create frequency details
        frequency_details = FrequencyDetailsType()
        frequency_details.start = datetime.combine(datetime(2000, 1, 1), start_time)
        
        if end_time:
            frequency_details.end = datetime.combine(datetime(2000, 1, 1), end_time)
        
        if intervals:
            frequency_details.intervals = intervals
        
        # Create schedule object
        schedule = ScheduleType()
        schedule.name = name
        schedule.priority = str(priority)
        schedule.type_value = schedule_type
        schedule.frequency = frequency
        schedule.execution_order = execution_order
        schedule.frequency_details = frequency_details
        
        uri = self._api_client._build_uri("schedules")
        request_body = self._api_client._get_object_as_request_content(schedule)
        
        response_content = self._api_client._api_request(
            uri, "POST", 200, session=session, body=request_body
        )
        
        return self._api_client._get_response_as_object(response_content, ScheduleType)
    
    @ApiVersionAttribute(2, 3)
    @OnPremiseOnlyAttribute()
    def create_hourly_schedule(self,
                              session: TableauSession,
                              name: str,
                              priority: int,
                              schedule_type: ScheduleTypeType,
                              execution_order: ScheduleTypeExecutionOrder,
                              start_time: time,
                              end_time: Optional[time] = None,
                              minutes_between_jobs: Optional[IntervalTypeMinutes] = None,
                              hours_between_jobs: Optional[IntervalTypeHours] = None) -> ScheduleType:
        """
        Creates a new hourly schedule on Tableau Server. Versions 2.3+
        
        Args:
            session: TableauSession object
            name: The name of the schedule
            priority: Priority value between 1 and 100
            schedule_type: Extract or Subscription schedule type
            execution_order: Parallel or Serial execution
            start_time: The time of day to start
            end_time: The time of day to stop (optional)
            minutes_between_jobs: Minutes between jobs (optional)
            hours_between_jobs: Hours between jobs (optional)
            
        Returns:
            ScheduleType: The created schedule
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            TableauOnlineNotSupportedException: If used with Tableau Online
            ValueError: If parameters are invalid
        """
        intervals = []
        
        if minutes_between_jobs:
            interval = IntervalType()
            interval.minutes = minutes_between_jobs
            intervals.append(interval)
        
        if hours_between_jobs:
            interval = IntervalType()
            interval.hours = hours_between_jobs
            intervals.append(interval)
        
        return self._create_schedule(
            session, name, priority, ScheduleTypeFrequency.HOURLY,
            schedule_type, execution_order, start_time, end_time, intervals
        )
    
    @ApiVersionAttribute(2, 3)
    @OnPremiseOnlyAttribute()
    def create_daily_schedule(self,
                             session: TableauSession,
                             name: str,
                             priority: int,
                             schedule_type: ScheduleTypeType,
                             execution_order: ScheduleTypeExecutionOrder,
                             start_time: time) -> ScheduleType:
        """
        Creates a new daily schedule on Tableau Server. Versions 2.3+
        
        Args:
            session: TableauSession object
            name: The name of the schedule
            priority: Priority value between 1 and 100
            schedule_type: Extract or Subscription schedule type
            execution_order: Parallel or Serial execution
            start_time: The time of day to run
            
        Returns:
            ScheduleType: The created schedule
        """
        return self._create_schedule(
            session, name, priority, ScheduleTypeFrequency.DAILY,
            schedule_type, execution_order, start_time, None, []
        )
    
    @ApiVersionAttribute(2, 3)
    @OnPremiseOnlyAttribute()
    def create_weekly_schedule(self,
                              session: TableauSession,
                              name: str,
                              priority: int,
                              schedule_type: ScheduleTypeType,
                              execution_order: ScheduleTypeExecutionOrder,
                              start_time: time,
                              day_to_run: IntervalTypeWeekDay) -> ScheduleType:
        """
        Creates a new weekly schedule on Tableau Server. Versions 2.3+
        
        Args:
            session: TableauSession object
            name: The name of the schedule
            priority: Priority value between 1 and 100
            schedule_type: Extract or Subscription schedule type
            execution_order: Parallel or Serial execution
            start_time: The time of day to run
            day_to_run: The day of week to run on
            
        Returns:
            ScheduleType: The created schedule
        """
        interval = IntervalType()
        interval.week_day = day_to_run
        
        return self._create_schedule(
            session, name, priority, ScheduleTypeFrequency.WEEKLY,
            schedule_type, execution_order, start_time, None, [interval]
        )
    
    @ApiVersionAttribute(2, 3)
    @OnPremiseOnlyAttribute()
    def create_monthly_schedule(self,
                               session: TableauSession,
                               name: str,
                               priority: int,
                               schedule_type: ScheduleTypeType,
                               execution_order: ScheduleTypeExecutionOrder,
                               start_time: time) -> ScheduleType:
        """
        Creates a new monthly schedule on Tableau Server. Versions 2.3+
        
        Args:
            session: TableauSession object
            name: The name of the schedule
            priority: Priority value between 1 and 100
            schedule_type: Extract or Subscription schedule type
            execution_order: Parallel or Serial execution
            start_time: The time of day to run
            
        Returns:
            ScheduleType: The created schedule
        """
        return self._create_schedule(
            session, name, priority, ScheduleTypeFrequency.MONTHLY,
            schedule_type, execution_order, start_time, None, []
        )
    
    @ApiVersionAttribute(2, 2)
    @OnPremiseOnlyAttribute()
    def query_extract_refresh_tasks(self,
                                   session: TableauSession,
                                   schedule_id: str,
                                   page_size: int = 100,
                                   page_number: int = 1) -> Tuple[PaginationType, List[ExtractType]]:
        """
        Returns a list of the extract refresh tasks for a specified schedule. Versions 2.2+
        
        Args:
            session: TableauSession object
            schedule_id: The ID of the schedule to get extract information for
            page_size: The number of items to return (1-1000, default: 100)
            page_number: The offset for paging (default: 1)
            
        Returns:
            Tuple containing pagination info and list of extracts
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            TableauOnlineNotSupportedException: If used with Tableau Online
            ValueError: If parameters are invalid
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session), 
            ("schedule_id", schedule_id)
        )
        self._api_client._check_parameters_between(
            ("page_size", page_size, 1, 1000),
            ("page_number", page_number, 1, float('inf'))
        )
        
        uri = self._api_client._build_uri(
            f"sites/{session.site_id}/schedules/{schedule_id}/extracts",
            ("pageSize", str(page_size)),
            ("pageNumber", str(page_number))
        )
        
        response_content = self._api_client._api_request(
            uri, "GET", 200, session=session, body=None
        )
        
        pagination, extract_list = self._api_client._get_response_as_objects(
            response_content, PaginationType, ExtractListType
        )
        
        extracts = extract_list.extract if extract_list and extract_list.extract else []
        return pagination, extracts
    
    @ApiVersionAttribute(2, 2)
    @OnPremiseOnlyAttribute()
    def query_schedules(self,
                       session: TableauSession,
                       page_size: int = 100,
                       page_number: int = 1) -> Tuple[PaginationType, List[ScheduleType]]:
        """
        Returns a list of extract and subscription schedules. Versions 2.2+
        
        Args:
            session: TableauSession object
            page_size: The number of items to return (1-1000, default: 100)
            page_number: The offset for paging (default: 1)
            
        Returns:
            Tuple containing pagination info and list of schedules
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            TableauOnlineNotSupportedException: If used with Tableau Online
            ValueError: If parameters are invalid
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(("session", session))
        self._api_client._check_parameters_between(
            ("page_size", page_size, 1, 1000),
            ("page_number", page_number, 1, float('inf'))
        )
        
        uri = self._api_client._build_uri(
            "schedules",
            ("pageSize", str(page_size)),
            ("pageNumber", str(page_number))
        )
        
        response_content = self._api_client._api_request(
            uri, "GET", 200, session=session, body=None
        )
        
        pagination, schedule_list = self._api_client._get_response_as_objects(
            response_content, PaginationType, ScheduleListType
        )
        
        schedules = schedule_list.schedule if schedule_list and schedule_list.schedule else []
        return pagination, schedules
    
    @ApiVersionAttribute(2, 3)
    @OnPremiseOnlyAttribute()
    def delete_schedule(self, session: TableauSession, schedule_id: str) -> None:
        """
        Deletes the specified schedule. Versions 2.3+
        
        Args:
            session: TableauSession object
            schedule_id: The ID of the schedule to delete
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            TableauOnlineNotSupportedException: If used with Tableau Online
            ValueError: If required parameters are missing
        """
        self._api_client._check_endpoint_availability()
        self._api_client._check_null_parameters(
            ("session", session), 
            ("schedule_id", schedule_id)
        )
        
        uri = self._api_client._build_uri(f"schedules/{schedule_id}")
        
        self._api_client._api_request(
            uri, "DELETE", 204, session=session, body=None
        )
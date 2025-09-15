from typing import Optional, List, Tuple, TYPE_CHECKING
from models.ts_api import (
    SubscriptionType, SubscriptionListType, SubscriptionContentType, 
    SubscriptionContentTypeType, ScheduleType, UserType, PaginationType
)
from models.tableau_session import TableauSession
from attributes.api_version_attribute import ApiVersionAttribute

if TYPE_CHECKING:
    from tableau_api_client import TableauApiClient


class TableauSubscriptionsClient:
    """Client for Tableau Server subscriptions operations"""
    
    def __init__(self, api_client: "TableauApiClient"):
        """
        Initialize subscriptions client
        
        Args:
            api_client: Instance of TableauApiClient
        """
        self._api_client = api_client
    
    @ApiVersionAttribute(2, 3)
    def create_subscription(self,
                           session: TableauSession,
                           subscription_subject: str,
                           content_type: SubscriptionContentTypeType,
                           content_id: str,
                           schedule_id: str,
                           user_id: str) -> SubscriptionType:
        """
        Creates a new subscription to a view or workbook for a specific user. When a user is 
        subscribed to the content, Tableau Server sends the content to the user in email on 
        the schedule that's defined in Tableau Server. Versions 2.3+
        
        Args:
            session: The session to use
            subscription_subject: A description for the subscription. This description is displayed 
                                when users list subscriptions for a site in the server environment
            content_type: Workbook to create a subscription for a workbook, or View to create 
                         a subscription for a view
            content_id: The ID of the workbook or view to subscribe to
            schedule_id: The ID of a schedule to associate the subscription with
            user_id: The ID of the user to create the subscription for. Note: The user must 
                    have an email address defined in Tableau Server
            
        Returns:
            SubscriptionType: The created subscription object
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("subscription_subject", subscription_subject),
            ("content_id", content_id),
            ("schedule_id", schedule_id),
            ("user_id", user_id)
        )
        
        # Create subscription object
        subscription = SubscriptionType()
        subscription.subject = subscription_subject
        
        # Create content object
        content = SubscriptionContentType()
        content.type_value = content_type
        content.id = content_id
        subscription.content = content
        
        # Create schedule object
        schedule = ScheduleType()
        schedule.id = schedule_id
        subscription.schedule = schedule
        
        # Create user object
        user = UserType()
        user.id = user_id
        subscription.user = user
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/subscriptions")
        request_body = self._api_client.get_object_as_request_content(subscription)
        
        response_content = self._api_client.api_request(
            uri, "POST", 201, session=session, body=request_body
        )
        
        # Parse response
        return self._api_client.get_response_as_object(response_content, SubscriptionType)
    
    @ApiVersionAttribute(2, 3)
    def query_subscription(self, session: TableauSession, subscription_id: str) -> SubscriptionType:
        """
        Returns information about the specified subscription. Versions 2.3+
        
        Args:
            session: The session to use
            subscription_id: The ID of the subscription to query
            
        Returns:
            SubscriptionType: The subscription object
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("subscription_id", subscription_id)
        )
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/subscriptions/{subscription_id}")
        
        response_content = self._api_client.api_request(
            uri, "GET", 200, session=session, body=None
        )
        
        # Parse response
        return self._api_client.get_response_as_object(response_content, SubscriptionType)
    
    @ApiVersionAttribute(2, 3)
    def query_subscriptions(self,
                           session: TableauSession,
                           page_size: int = 100,
                           page_number: int = 1) -> Tuple[PaginationType, List[SubscriptionType]]:
        """
        Returns a list of all the subscriptions on the specified site. Versions 2.3+
        
        Args:
            session: The session to use
            page_size: The number of items to return in one response. The minimum is 1. 
                      The maximum is 1000. The default is 100
            page_number: The offset for paging. The default is 1
            
        Returns:
            Tuple containing pagination info and list of subscriptions
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing or out of range
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(("session", session))
        self._api_client.check_parameters_between(
            ("page_size", page_size, 1, 1000),
            ("page_number", page_number, 1, 2147483647)
        )
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/subscriptions",
                                        ("pageSize", page_size),
                                        ("pageNumber", page_number))
        
        response_content = self._api_client.api_request(
            uri, "GET", 200, session=session, body=None
        )
        
        # Parse response
        pagination, subscription_list = self._api_client.get_response_as_objects(
            response_content, PaginationType, SubscriptionListType
        )
        
        subscriptions = subscription_list.subscription if subscription_list and subscription_list.subscription else []
        return pagination, subscriptions
    
    @ApiVersionAttribute(2, 3)
    def update_subscription(self,
                           session: TableauSession,
                           subscription_id: str,
                           new_subject: Optional[str] = None,
                           new_schedule_id: Optional[str] = None) -> SubscriptionType:
        """
        Modifies an existing subscription, allowing you to change the subject or schedule 
        for the subscription. Versions 2.3+
        
        Args:
            session: The session to use
            subscription_id: The ID of the subscription to update
            new_subject: A new subject for the subscription
            new_schedule_id: The ID of a schedule to associate this subscription with
            
        Returns:
            SubscriptionType: The updated subscription object
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("subscription_id", subscription_id)
        )
        
        # Create subscription object with updates
        subscription = SubscriptionType()
        if new_subject is not None:
            subscription.subject = new_subject
        
        if new_schedule_id is not None:
            schedule = ScheduleType()
            schedule.id = new_schedule_id
            subscription.schedule = schedule
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/subscriptions/{subscription_id}")
        request_body = self._api_client.get_object_as_request_content(subscription)
        
        response_content = self._api_client.api_request(
            uri, "PUT", 200, session=session, body=request_body
        )
        
        # Parse response
        return self._api_client.get_response_as_object(response_content, SubscriptionType)
    
    @ApiVersionAttribute(2, 3)
    def delete_subscription(self, session: TableauSession, subscription_id: str) -> None:
        """
        Deletes the specified subscription. Versions 2.3+
        
        Args:
            session: The session to use
            subscription_id: The ID of the subscription to delete
            
        Raises:
            TableauRequestException: If the request fails
            TableauApiVersionException: If API version is too low
            ValueError: If required parameters are missing
        """
        self._api_client.check_endpoint_availability()
        self._api_client.check_null_parameters(
            ("session", session),
            ("subscription_id", subscription_id)
        )
        
        # Build URI and make request
        uri = self._api_client.build_uri(f"sites/{session.site_id}/subscriptions/{subscription_id}")
        
        self._api_client.api_request(
            uri, "DELETE", 204, session=session, body=None
        )
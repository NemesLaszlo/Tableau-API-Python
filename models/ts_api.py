from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Optional, Union
from xsdata.models.datatype import XmlDateTime, XmlTime

__NAMESPACE__ = "http://tableau.com/api"


class AccelerationStatusType(Enum):
    ACCELERATED = 'Accelerated'
    ADMIN_SUSPENDED = 'AdminSuspended'
    AUTO_ACCELERATED = 'AutoAccelerated'
    FAILED = 'Failed'
    IN_PROGRESS = 'InProgress'
    NOT_USEFUL = 'NotUseful'
    OFF = 'Off'
    ON = 'On'
    PENDING = 'Pending'
    SKIPPED_DUE_TO_RESOURCE_CONSTRAINT = 'SkippedDueToResourceConstraint'
    SYSTEM_SUSPENDED = 'SystemSuspended'
    UNKNOWN = 'Unknown'
    WAITING = 'Waiting'
@dataclass
class AssociatedUserLuidListType:
    class Meta:
        name = "associatedUserLuidListType"

    associated_user_luid: list[str] = field(
        default_factory=list,
        metadata={
            "name": "associatedUserLuid",
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class AssociatedUserLuidMappingType:
    class Meta:
        name = "associatedUserLuidMappingType"

    user_luid_pair: list["AssociatedUserLuidMappingType.UserLuidPair"] = field(
        default_factory=list,
        metadata={
            "name": "userLuidPair",
            "type": "Element",
            "namespace": "",
        }
    )

    @dataclass
    class UserLuidPair:
        destination_site_user_luid: Optional[str] = field(
            default=None,
            metadata={
                "name": "destinationSiteUserLuid",
                "type": "Attribute",
            }
        )
        source_site_user_luid: Optional[str] = field(
            default=None,
            metadata={
                "name": "sourceSiteUserLuid",
                "type": "Attribute",
            }
        )
class BackgroundJobTypeStatus(Enum):
    CANCELLED = 'Cancelled'
    FAILED = 'Failed'
    IN_PROGRESS = 'InProgress'
    PENDING = 'Pending'
    SUCCESS = 'Success'
@dataclass
class BroadcastViewSendType:
    class Meta:
        name = "broadcastViewSendType"

    allow_downloads: Optional[bool] = field(
        default=None,
        metadata={
            "name": "allowDownloads",
            "type": "Attribute",
        }
    )
    show_tabs: Optional[bool] = field(
        default=None,
        metadata={
            "name": "showTabs",
            "type": "Attribute",
        }
    )
    show_watermark: Optional[bool] = field(
        default=None,
        metadata={
            "name": "showWatermark",
            "type": "Attribute",
        }
    )
    view_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "viewId",
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
class CapabilityTypeMode(Enum):
    ALLOW = 'Allow'
    DENY = 'Deny'
class CapabilityTypeName(Enum):
    ADD_COMMENT = 'AddComment'
    CHANGE_HIERARCHY = 'ChangeHierarchy'
    CHANGE_PERMISSIONS = 'ChangePermissions'
    CONNECT = 'Connect'
    CREATE_REFRESH_METRICS = 'CreateRefreshMetrics'
    DELETE = 'Delete'
    EXECUTE = 'Execute'
    EXPORT_DATA = 'ExportData'
    EXPORT_IMAGE = 'ExportImage'
    EXPORT_XML = 'ExportXml'
    FILTER = 'Filter'
    PROJECT_LEADER = 'ProjectLeader'
    READ = 'Read'
    RUN_EXPLAIN_DATA = 'RunExplainData'
    SAVE_AS = 'SaveAs'
    SHARE_VIEW = 'ShareView'
    VIEW_COMMENTS = 'ViewComments'
    VIEW_UNDERLYING_DATA = 'ViewUnderlyingData'
    WEB_AUTHORING = 'WebAuthoring'
    WEB_AUTHORING_FOR_FLOWS = 'WebAuthoringForFlows'
    WRITE = 'Write'
@dataclass
class ConnectedApplicationProjectListType:
    class Meta:
        name = "connectedApplicationProjectListType"

    project_id: list[str] = field(
        default_factory=list,
        metadata={
            "name": "projectId",
            "type": "Element",
            "namespace": "",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
@dataclass
class ConnectedApplicationSecretType:
    class Meta:
        name = "connectedApplicationSecretType"

    created_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "createdAt",
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    value: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
@dataclass
class ConnectionCredentialsType:
    class Meta:
        name = "connectionCredentialsType"

    embed: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    o_auth: Optional[str] = field(
        default=None,
        metadata={
            "name": "oAuth",
            "type": "Attribute",
        }
    )
    password: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
@dataclass
class ConnectionParamsForAnchorType:
    class Meta:
        name = "connectionParamsForAnchorType"

    base64xml: Optional[str] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
class ContentActionTypeAction(Enum):
    MOVE_DATABASE_AND_ALL_TABLES = 'MoveDatabaseAndAllTables'
    MOVE_DATABASE_ONLY = 'MoveDatabaseOnly'
@dataclass
class ContentTypeAndIdType:
    class Meta:
        name = "contentTypeAndIdType"

    content_type: Optional[str] = field(
        default=None,
        metadata={
            "name": "contentType",
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
@dataclass
class ContentsCountsType:
    class Meta:
        name = "contentsCountsType"

    datasource_count: Optional[int] = field(
        default=None,
        metadata={
            "name": "datasourceCount",
            "type": "Attribute",
        }
    )
    project_count: Optional[int] = field(
        default=None,
        metadata={
            "name": "projectCount",
            "type": "Attribute",
        }
    )
    view_count: Optional[int] = field(
        default=None,
        metadata={
            "name": "viewCount",
            "type": "Attribute",
        }
    )
    workbook_count: Optional[int] = field(
        default=None,
        metadata={
            "name": "workbookCount",
            "type": "Attribute",
        }
    )
class DataAlertCreateAlertTypeAlertCondition(Enum):
    ABOVE = 'above'
    ABOVE_EQUAL = 'above-equal'
    BELOW = 'below'
    BELOW_EQUAL = 'below-equal'
    EQUAL = 'equal'
class DataAlertCreateAlertTypeDevice(Enum):
    DEFAULT = 'default'
    DESKTOP = 'desktop'
    PHONE = 'phone'
    TABLET = 'tablet'
class DataAlertCreateAlertTypeFrequency(Enum):
    AS_FREQUENTLY_AS_POSSIBLE = 'AsFrequentlyAsPossible'
    DAILY = 'Daily'
    HOURLY = 'Hourly'
    ONCE = 'Once'
    WEEKLY = 'Weekly'
class DataAlertCreateAlertTypeVisibility(Enum):
    PRIVATE = 'private'
    PUBLIC = 'public'
class DataAlertTypeFrequency(Enum):
    AS_FREQUENTLY_AS_POSSIBLE = 'AsFrequentlyAsPossible'
    DAILY = 'Daily'
    HOURLY = 'Hourly'
    ONCE = 'Once'
    WEEKLY = 'Weekly'
@dataclass
class DataAlertsRecipientType:
    class Meta:
        name = "dataAlertsRecipientType"

    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    last_sent: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "lastSent",
            "type": "Attribute",
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
class DataFreshnessPolicyTypeOption(Enum):
    ALWAYS_LIVE = 'AlwaysLive'
    FRESH_AT = 'FreshAt'
    FRESH_EVERY = 'FreshEvery'
    SITE_DEFAULT = 'SiteDefault'
@dataclass
class DataQualityTriggerType:
    class Meta:
        name = "dataQualityTriggerType"

    active: Optional[bool] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    content_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "contentId",
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    content_type: Optional[str] = field(
        default=None,
        metadata={
            "name": "contentType",
            "type": "Attribute",
        }
    )
    created_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "createdAt",
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    message: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    severe: Optional[bool] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    site_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "siteId",
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    type_value: Optional[str] = field(
        default=None,
        metadata={
            "name": "type",
            "type": "Attribute",
        }
    )
    updated_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "updatedAt",
            "type": "Attribute",
        }
    )
    user_display_name: Optional[str] = field(
        default=None,
        metadata={
            "name": "userDisplayName",
            "type": "Attribute",
        }
    )
    user_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "userId",
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
class DataSourceTypeParentType(Enum):
    DATA_ROLE = 'DataRole'
class DataUpdateConstConditionTypeType(Enum):
    BOOLEAN = 'boolean'
    DATETIME = 'datetime'
    DOUBLE = 'double'
    INTEGER = 'integer'
    STRING = 'string'
class DatabaseTypeType(Enum):
    CLOUD_FILE = 'CloudFile'
    DATABASE_SERVER = 'DatabaseServer'
    FILE = 'File'
    WEB_DATA_CONNECTOR = 'WebDataConnector'
class DatabaseTypeContentPermissions(Enum):
    LOCKED_TO_DATABASE = 'LockedToDatabase'
    MANAGED_BY_OWNER = 'ManagedByOwner'
@dataclass
class DegradationType:
    class Meta:
        name = "degradationType"

    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    severity: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
@dataclass
class DomainDirectiveType:
    class Meta:
        name = "domainDirectiveType"

    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "required": True,
        }
    )
    short_name: Optional[str] = field(
        default=None,
        metadata={
            "name": "shortName",
            "type": "Attribute",
        }
    )
@dataclass
class EmbeddingSettingsType:
    class Meta:
        name = "embeddingSettingsType"

    allow_list: Optional[str] = field(
        default=None,
        metadata={
            "name": "allowList",
            "type": "Attribute",
            "required": True,
        }
    )
    unrestricted_embedding: Optional[bool] = field(
        default=None,
        metadata={
            "name": "unrestrictedEmbedding",
            "type": "Attribute",
            "required": True,
        }
    )
@dataclass
class EncryptedKeychainListType:
    class Meta:
        name = "encryptedKeychainListType"

    encrypted_keychain: list[str] = field(
        default_factory=list,
        metadata={
            "name": "encryptedKeychain",
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class ErrorType:
    class Meta:
        name = "errorType"

    callstack: Optional[str] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    detail: Optional[str] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    summary: Optional[str] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    code: Optional[int] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "required": True,
        }
    )
class ExplanationTypeType(Enum):
    DATA_MONITORING_CANDIDATE = 'data_monitoring_candidate'
    POPULAR = 'popular'
    SIMILAR_USERS = 'similar_users'
@dataclass
class ExtensionUrlStatusType:
    class Meta:
        name = "extensionUrlStatusType"

    status: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
@dataclass
class ExtensionsSafeListEntry:
    class Meta:
        name = "extensionsSafeListEntry"

    full_data_allowed: Optional[bool] = field(
        default=None,
        metadata={
            "name": "fullDataAllowed",
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    prompt_needed: Optional[bool] = field(
        default=None,
        metadata={
            "name": "promptNeeded",
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    url: Optional[str] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
@dataclass
class ExtensionsServerSettingsType:
    class Meta:
        name = "extensionsServerSettingsType"

    block_list: list[str] = field(
        default_factory=list,
        metadata={
            "name": "blockList",
            "type": "Element",
            "namespace": "",
        }
    )
    extensions_globally_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "extensionsGloballyEnabled",
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
@dataclass
class ExternalAuthorizationServerType:
    class Meta:
        name = "externalAuthorizationServerType"

    created_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "createdAt",
            "type": "Attribute",
        }
    )
    enabled: Optional[bool] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    issuer_url: Optional[str] = field(
        default=None,
        metadata={
            "name": "issuerUrl",
            "type": "Attribute",
        }
    )
    jwks_uri: Optional[str] = field(
        default=None,
        metadata={
            "name": "jwksUri",
            "type": "Attribute",
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
class ExtractTypeType(Enum):
    FULL_REFRESH = 'FullRefresh'
    INCREMENTAL_REFRESH = 'IncrementalRefresh'
class FavoriteTypeType(Enum):
    COLLECTION = 'collection'
    DATAROLE = 'datarole'
    DATASOURCE = 'datasource'
    FLOW = 'flow'
    METRIC = 'metric'
    PROJECT = 'project'
    VIEW = 'view'
    WORKBOOK = 'workbook'
@dataclass
class FileUploadType:
    class Meta:
        name = "fileUploadType"

    file_size: Optional[str] = field(
        default=None,
        metadata={
            "name": "fileSize",
            "type": "Attribute",
        }
    )
    upload_session_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "uploadSessionId",
            "type": "Attribute",
            "required": True,
            "pattern": r'([0-9]+:[0-9a-fA-F]+)-([0-9]+:[0-9]+)',
        }
    )
@dataclass
class FlowOutputStepType:
    class Meta:
        name = "flowOutputStepType"

    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
@dataclass
class FlowParameterDomainType:
    class Meta:
        name = "flowParameterDomainType"

    domain_type: Optional[str] = field(
        default=None,
        metadata={
            "name": "domainType",
            "type": "Attribute",
        }
    )
@dataclass
class FlowParameterListValueListType:
    class Meta:
        name = "flowParameterListValueListType"

    value: list[str] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class FlowParameterRunType:
    class Meta:
        name = "flowParameterRunType"

    description: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    override_value: Optional[str] = field(
        default=None,
        metadata={
            "name": "overrideValue",
            "type": "Attribute",
        }
    )
    parameter_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "parameterId",
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
@dataclass
class FlowParameterSpecType:
    class Meta:
        name = "flowParameterSpecType"

    override_value: Optional[str] = field(
        default=None,
        metadata={
            "name": "overrideValue",
            "type": "Attribute",
        }
    )
    parameter_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "parameterId",
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
class FlowRunSpecTypeRunMode(Enum):
    FULL = 'full'
    INCREMENTAL = 'incremental'
class FlowRunTypeStatus(Enum):
    CANCELLED = 'Cancelled'
    FAILED = 'Failed'
    IN_PROGRESS = 'InProgress'
    PENDING = 'Pending'
    SUCCESS = 'Success'
class FlowWarningsListContainerTypeRunMode(Enum):
    FULL = 'full'
    INCREMENTAL = 'incremental'
class FreshAtScheduleTypeFrequency(Enum):
    DAY = 'Day'
    MONTH = 'Month'
    WEEK = 'Week'
class FreshEveryScheduleTypeFrequency(Enum):
    DAYS = 'Days'
    HOURS = 'Hours'
    MINUTES = 'Minutes'
    WEEKS = 'Weeks'
@dataclass
class GenerativeAiRegistrationType:
    class Meta:
        name = "generativeAiRegistrationType"

    domain: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    orgid: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    registered: Optional[bool] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    username: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
class ImportDirectiveTypeGrantLicenseMode(Enum):
    ON_LOGIN = 'onLogin'
    ON_SYNC = 'onSync'
class ImportSourceType(Enum):
    ACTIVE_DIRECTORY = 'ActiveDirectory'
class IntervalTypeHours(Enum):
    VALUE_1 = 1
    VALUE_12 = 12
    VALUE_2 = 2
    VALUE_4 = 4
    VALUE_6 = 6
    VALUE_8 = 8
class IntervalTypeMinutes(Enum):
    VALUE_15 = 15
    VALUE_30 = 30
class IntervalTypeValue(Enum):
    LAST_DAY = 'LastDay'
class IntervalTypeWeekDay(Enum):
    FRIDAY = 'Friday'
    MONDAY = 'Monday'
    SATURDAY = 'Saturday'
    SUNDAY = 'Sunday'
    THURSDAY = 'Thursday'
    TUESDAY = 'Tuesday'
    WEDNESDAY = 'Wednesday'
class JobTypeMode(Enum):
    ASYNCHRONOUS = 'Asynchronous'
class JobTypeType(Enum):
    BROADCAST_POST = 'BroadcastPost'
    GROUP_SYNC = 'GroupSync'
    PUBLISH_DATASOURCE = 'PublishDatasource'
    PUBLISH_WORKBOOK = 'PublishWorkbook'
    REFRESH_EXTRACT = 'RefreshExtract'
    RUN_FLOW = 'RunFlow'
@dataclass
class LabelCategoryType:
    class Meta:
        name = "labelCategoryType"

    description: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
class LicensingRoleType(Enum):
    GUEST = 'Guest'
    INTERACTOR = 'Interactor'
    UNLICENSED = 'Unlicensed'
    VIEWER = 'Viewer'
@dataclass
class LinkSiteMigrationType:
    class Meta:
        name = "linkSiteMigrationType"

    site_migration_job_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "siteMigrationJobId",
            "type": "Attribute",
        }
    )
    tenant_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "tenantId",
            "type": "Attribute",
        }
    )
class LinkUserIdptype(Enum):
    DEFAULT = 'DEFAULT'
    OPEN_ID = 'OpenID'
    SAML = 'SAML'
    TABLEAU_IDWITH_MFA = 'TableauIDWithMFA'
@dataclass
class LinkUserOperationResultType:
    class Meta:
        name = "linkUserOperationResultType"

    result_codes: Optional[str] = field(
        default=None,
        metadata={
            "name": "resultCodes",
            "type": "Attribute",
            "required": True,
        }
    )
    user_id: Optional[int] = field(
        default=None,
        metadata={
            "name": "userId",
            "type": "Attribute",
        }
    )
    user_luid: Optional[str] = field(
        default=None,
        metadata={
            "name": "userLuid",
            "type": "Attribute",
        }
    )
class LinkUserOperationType(Enum):
    ADD_SITE_ROLE = 'ADD_SITE_ROLE'
    CREATE_USER_SITE_ROLE = 'CREATE_USER_SITE_ROLE'
    DELETE_SITE_ROLE = 'DELETE_SITE_ROLE'
    UNMERGE_SITE_ROLE = 'UNMERGE_SITE_ROLE'
    UPDATE_SITE_ROLE = 'UPDATE_SITE_ROLE'
@dataclass
class LinkUserType:
    class Meta:
        name = "linkUserType"

    email: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
class LinkedTaskJobTypeStatus(Enum):
    CANCELLED = 'Cancelled'
    FAILED = 'Failed'
    IN_PROGRESS = 'InProgress'
    PENDING = 'Pending'
    SUCCESS = 'Success'
class LocationTypeType(Enum):
    PERSONAL_SPACE = 'PersonalSpace'
    PROJECT = 'Project'
class NotificationPreferenceUpdateStatusTypeStatus(Enum):
    FAILED = 'Failed'
    SUCCESS = 'Success'
@dataclass
class PaginationType:
    class Meta:
        name = "paginationType"

    page_number: Optional[int] = field(
        default=None,
        metadata={
            "name": "pageNumber",
            "type": "Attribute",
            "required": True,
        }
    )
    page_size: Optional[int] = field(
        default=None,
        metadata={
            "name": "pageSize",
            "type": "Attribute",
            "required": True,
        }
    )
    total_available: Optional[int] = field(
        default=None,
        metadata={
            "name": "totalAvailable",
            "type": "Attribute",
            "required": True,
        }
    )
class ParentTypeType(Enum):
    PROJECT = 'Project'
@dataclass
class PersonalAccessTokenType:
    class Meta:
        name = "personalAccessTokenType"

    expires_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "expiresAt",
            "type": "Attribute",
        }
    )
    last_used_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "lastUsedAt",
            "type": "Attribute",
        }
    )
    token_guid: Optional[str] = field(
        default=None,
        metadata={
            "name": "tokenGuid",
            "type": "Attribute",
        }
    )
    token_name: Optional[str] = field(
        default=None,
        metadata={
            "name": "tokenName",
            "type": "Attribute",
        }
    )
@dataclass
class PersonalSpaceType:
    class Meta:
        name = "personalSpaceType"

    luid: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    owner_luid: Optional[str] = field(
        default=None,
        metadata={
            "name": "ownerLuid",
            "type": "Attribute",
        }
    )
    read_only: Optional[bool] = field(
        default=None,
        metadata={
            "name": "readOnly",
            "type": "Attribute",
        }
    )
@dataclass
class ProductVersion:
    class Meta:
        name = "productVersion"

    value: str = field(
        default='',
        metadata={
            "required": True,
        }
    )
    build: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9]{5}.[0-9]{2}.[0-9]{4}.[0-9]{4}',
        }
    )
class ProjectTypeContentPermissions(Enum):
    LOCKED_TO_PROJECT = 'LockedToProject'
    LOCKED_TO_PROJECT_WITHOUT_NESTED = 'LockedToProjectWithoutNested'
    MANAGED_BY_OWNER = 'ManagedByOwner'
@dataclass
class PublishToSalesforceInfoType:
    class Meta:
        name = "publishToSalesforceInfoType"

    message: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    status_code: Optional[int] = field(
        default=None,
        metadata={
            "name": "statusCode",
            "type": "Attribute",
        }
    )
    view_luid: Optional[str] = field(
        default=None,
        metadata={
            "name": "viewLuid",
            "type": "Attribute",
        }
    )
@dataclass
class PublishToSalesforceRequestType:
    class Meta:
        name = "publishToSalesforceRequestType"

    view_luids: list[str] = field(
        default_factory=list,
        metadata={
            "name": "viewLuids",
            "type": "Element",
            "namespace": "",
            "min_occurs": 1,
        }
    )
    oauth_id: Optional[int] = field(
        default=None,
        metadata={
            "name": "oauthId",
            "type": "Attribute",
        }
    )
    salesforce_app_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "salesforceAppId",
            "type": "Attribute",
        }
    )
class RecommendedContentType(Enum):
    VIEW = 'view'
class RevisionLimitTypeValue(Enum):
    VALUE_MINUS_1 = -1
@dataclass
class SalesforceAppType:
    class Meta:
        name = "salesforceAppType"

    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    is_private_app: Optional[bool] = field(
        default=None,
        metadata={
            "name": "isPrivateApp",
            "type": "Attribute",
        }
    )
    label: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    namespace: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    url: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
class ScheduleTypeExecutionOrder(Enum):
    PARALLEL = 'Parallel'
    SERIAL = 'Serial'
class ScheduleTypeFrequency(Enum):
    DAILY = 'Daily'
    HOURLY = 'Hourly'
    MONTHLY = 'Monthly'
    WEEKLY = 'Weekly'
class ScheduleTypeType(Enum):
    ACTIVE_DIRECTORY_SYNC = 'ActiveDirectorySync'
    DATA_ACCELERATION = 'DataAcceleration'
    EXTRACT = 'Extract'
    FLOW = 'Flow'
    SUBSCRIPTION = 'Subscription'
@dataclass
class ServerSettings:
    class Meta:
        name = "serverSettings"

    o_auth_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "oAuthEnabled",
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    offline_interaction_supported_phase: Optional[int] = field(
        default=None,
        metadata={
            "name": "offlineInteractionSupportedPhase",
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    sheet_image_max_age_ceiling: Optional[int] = field(
        default=None,
        metadata={
            "name": "sheetImageMaxAgeCeiling",
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    sheet_image_max_age_floor: Optional[int] = field(
        default=None,
        metadata={
            "name": "sheetImageMaxAgeFloor",
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
@dataclass
class ServiceTokenType:
    class Meta:
        name = "serviceTokenType"

    ancestor_pat_guid: Optional[str] = field(
        default=None,
        metadata={
            "name": "ancestorPatGuid",
            "type": "Attribute",
        }
    )
    expires_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "expiresAt",
            "type": "Attribute",
        }
    )
    last_used_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "lastUsedAt",
            "type": "Attribute",
        }
    )
    owner_luid: Optional[str] = field(
        default=None,
        metadata={
            "name": "ownerLuid",
            "type": "Attribute",
        }
    )
    status_code: Optional[int] = field(
        default=None,
        metadata={
            "name": "statusCode",
            "type": "Attribute",
        }
    )
    token_guid: Optional[str] = field(
        default=None,
        metadata={
            "name": "tokenGuid",
            "type": "Attribute",
        }
    )
    token_name: Optional[str] = field(
        default=None,
        metadata={
            "name": "tokenName",
            "type": "Attribute",
        }
    )
    token_secret: Optional[str] = field(
        default=None,
        metadata={
            "name": "tokenSecret",
            "type": "Attribute",
        }
    )
class SeverityLevelType(Enum):
    CRITICAL = 'critical'
    ERROR = 'error'
    WARN = 'warn'
@dataclass
class SiteEncryptionMetadata:
    class Meta:
        name = "siteEncryptionMetadata"

    key_created_date: Optional[str] = field(
        default=None,
        metadata={
            "name": "keyCreatedDate",
            "type": "Attribute",
        }
    )
    key_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "keyId",
            "type": "Attribute",
        }
    )
    key_rotated_date: Optional[str] = field(
        default=None,
        metadata={
            "name": "keyRotatedDate",
            "type": "Attribute",
        }
    )
    key_version_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "keyVersionId",
            "type": "Attribute",
        }
    )
@dataclass
class SiteOidcconfigurationType:
    class Meta:
        name = "siteOIDCConfigurationType"

    allow_embedded_authentication: Optional[bool] = field(
        default=None,
        metadata={
            "name": "allowEmbeddedAuthentication",
            "type": "Attribute",
        }
    )
    authorization_endpoint: Optional[str] = field(
        default=None,
        metadata={
            "name": "authorizationEndpoint",
            "type": "Attribute",
        }
    )
    client_authentication: Optional[str] = field(
        default=None,
        metadata={
            "name": "clientAuthentication",
            "type": "Attribute",
        }
    )
    client_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "clientId",
            "type": "Attribute",
        }
    )
    client_secret: Optional[str] = field(
        default=None,
        metadata={
            "name": "clientSecret",
            "type": "Attribute",
        }
    )
    custom_scope: Optional[str] = field(
        default=None,
        metadata={
            "name": "customScope",
            "type": "Attribute",
        }
    )
    discovery_endpoint: Optional[str] = field(
        default=None,
        metadata={
            "name": "discoveryEndpoint",
            "type": "Attribute",
        }
    )
    email_mapping: Optional[str] = field(
        default=None,
        metadata={
            "name": "emailMapping",
            "type": "Attribute",
        }
    )
    enabled: Optional[bool] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    end_session_endpoint: Optional[str] = field(
        default=None,
        metadata={
            "name": "endSessionEndpoint",
            "type": "Attribute",
        }
    )
    essential_acr_values: Optional[str] = field(
        default=None,
        metadata={
            "name": "essentialAcrValues",
            "type": "Attribute",
        }
    )
    first_name_mapping: Optional[str] = field(
        default=None,
        metadata={
            "name": "firstNameMapping",
            "type": "Attribute",
        }
    )
    full_name_mapping: Optional[str] = field(
        default=None,
        metadata={
            "name": "fullNameMapping",
            "type": "Attribute",
        }
    )
    idp_configuration_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "idpConfigurationId",
            "type": "Attribute",
        }
    )
    idp_configuration_name: Optional[str] = field(
        default=None,
        metadata={
            "name": "idpConfigurationName",
            "type": "Attribute",
        }
    )
    jwks_uri: Optional[str] = field(
        default=None,
        metadata={
            "name": "jwksUri",
            "type": "Attribute",
        }
    )
    known_provider_alias: Optional[str] = field(
        default=None,
        metadata={
            "name": "knownProviderAlias",
            "type": "Attribute",
        }
    )
    last_name_mapping: Optional[str] = field(
        default=None,
        metadata={
            "name": "lastNameMapping",
            "type": "Attribute",
        }
    )
    prompt: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    salesforce_domain: Optional[str] = field(
        default=None,
        metadata={
            "name": "salesforceDomain",
            "type": "Attribute",
        }
    )
    test_login_url: Optional[str] = field(
        default=None,
        metadata={
            "name": "testLoginUrl",
            "type": "Attribute",
        }
    )
    token_endpoint: Optional[str] = field(
        default=None,
        metadata={
            "name": "tokenEndpoint",
            "type": "Attribute",
        }
    )
    use_full_name: Optional[bool] = field(
        default=None,
        metadata={
            "name": "useFullName",
            "type": "Attribute",
        }
    )
    userinfo_endpoint: Optional[str] = field(
        default=None,
        metadata={
            "name": "userinfoEndpoint",
            "type": "Attribute",
        }
    )
    voluntary_acr_values: Optional[str] = field(
        default=None,
        metadata={
            "name": "voluntaryAcrValues",
            "type": "Attribute",
        }
    )
class SiteRoleType(Enum):
    CREATOR = 'Creator'
    EXPLORER = 'Explorer'
    EXPLORER_CAN_PUBLISH = 'ExplorerCanPublish'
    GUEST = 'Guest'
    SERVER_ADMINISTRATOR = 'ServerAdministrator'
    SITE_ADMINISTRATOR_CREATOR = 'SiteAdministratorCreator'
    SITE_ADMINISTRATOR_EXPLORER = 'SiteAdministratorExplorer'
    SUPPORT_USER = 'SupportUser'
    UNLICENSED = 'Unlicensed'
    VIEWER = 'Viewer'
class SiteTypeAdminMode(Enum):
    CONTENT_AND_USERS = 'ContentAndUsers'
    CONTENT_ONLY = 'ContentOnly'
class SiteTypeAskDataMode(Enum):
    DISABLED_ALWAYS = 'DisabledAlways'
    DISABLED_BY_DEFAULT = 'DisabledByDefault'
class SiteUserAuthSettingType(Enum):
    OPEN_ID = 'OpenID'
    SAML = 'SAML'
    SERVER_DEFAULT = 'ServerDefault'
class StatusNoteTypeType(Enum):
    COUNT_OF_USERS_ADDED_TO_GROUP = 'CountOfUsersAddedToGroup'
    COUNT_OF_USERS_ADDED_TO_SITE = 'CountOfUsersAddedToSite'
    COUNT_OF_USERS_IN_ACTIVE_DIRECTORY_GROUP = 'CountOfUsersInActiveDirectoryGroup'
    COUNT_OF_USERS_INFORMATION_UPDATED = 'CountOfUsersInformationUpdated'
    COUNT_OF_USERS_PROCESSED = 'CountOfUsersProcessed'
    COUNT_OF_USERS_REMOVED_FROM_GROUP = 'CountOfUsersRemovedFromGroup'
    COUNT_OF_USERS_SITE_ROLE_UPDATED = 'CountOfUsersSiteRoleUpdated'
    COUNT_OF_USERS_SKIPPED = 'CountOfUsersSkipped'
    COUNT_OF_USERS_UNLICENSED = 'CountOfUsersUnlicensed'
    COUNT_OF_USERS_WITH_INSUFFICIENT_LICENSES = 'CountOfUsersWithInsufficientLicenses'
@dataclass
class StatusType:
    class Meta:
        name = "statusType"

    code: Optional[int] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "required": True,
        }
    )
    result: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
class SubscriptionContentTypeType(Enum):
    VIEW = 'View'
    WORKBOOK = 'Workbook'
@dataclass
class SubscriptionJobType:
    class Meta:
        name = "subscriptionJobType"

    notes: Optional[str] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    subscription_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "subscriptionId",
            "type": "Element",
            "namespace": "",
            "required": True,
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    subscription_subject: Optional[str] = field(
        default=None,
        metadata={
            "name": "subscriptionSubject",
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
@dataclass
class SuggestionFeedbackType:
    class Meta:
        name = "suggestionFeedbackType"

    biased_toxic_harmful: Optional[bool] = field(
        default=None,
        metadata={
            "name": "biasedToxicHarmful",
            "type": "Attribute",
        }
    )
    feedback: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    feedback_text: Optional[str] = field(
        default=None,
        metadata={
            "name": "feedbackText",
            "type": "Attribute",
        }
    )
    inaccurate: Optional[bool] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    inappropriate: Optional[bool] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    incomplete: Optional[bool] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    other: Optional[bool] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    suggestion_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "suggestionId",
            "type": "Attribute",
        }
    )
@dataclass
class SuggestionType:
    class Meta:
        name = "suggestionType"

    content_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "contentId",
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    content_type: Optional[str] = field(
        default=None,
        metadata={
            "name": "contentType",
            "type": "Attribute",
        }
    )
    suggestion_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "suggestionId",
            "type": "Attribute",
        }
    )
    suggestion_type: Optional[str] = field(
        default=None,
        metadata={
            "name": "suggestionType",
            "type": "Attribute",
        }
    )
    text: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
@dataclass
class TableAnchorType:
    class Meta:
        name = "tableAnchorType"

    full_name: Optional[str] = field(
        default=None,
        metadata={
            "name": "fullName",
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
@dataclass
class TagType:
    class Meta:
        name = "tagType"

    label: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "required": True,
        }
    )
@dataclass
class TasSiteOauthClientType:
    class Meta:
        name = "tasSiteOAuthClientType"

    client_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "clientId",
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    enabled: Optional[bool] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
@dataclass
class UserNotificationsPreferenceType:
    class Meta:
        name = "userNotificationsPreferenceType"

    channel: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    disabled_by_override: Optional[bool] = field(
        default=None,
        metadata={
            "name": "disabledByOverride",
            "type": "Attribute",
        }
    )
    enabled: Optional[bool] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    notification_type: Optional[str] = field(
        default=None,
        metadata={
            "name": "notificationType",
            "type": "Attribute",
        }
    )
@dataclass
class ViewListType:
    class Meta:
        name = "viewListType"

    view: list["ViewType"] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class VirtualConnectionSourceConnectionType:
    class Meta:
        name = "virtualConnectionSourceConnectionType"

    connection_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "connectionId",
            "type": "Attribute",
        }
    )
    database: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    db_class: Optional[str] = field(
        default=None,
        metadata={
            "name": "dbClass",
            "type": "Attribute",
        }
    )
    port: Optional[int] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    server: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    username: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
@dataclass
class WarningType:
    class Meta:
        name = "warningType"

    error_code: Optional[str] = field(
        default=None,
        metadata={
            "name": "errorCode",
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    message: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
class WebhookDestinationHttpTypeMethod(Enum):
    POST = 'POST'
@dataclass
class WebhookSourceEventDatasourceCreatedType:
    class Meta:
        name = "webhookSourceEventDatasourceCreatedType"
@dataclass
class WebhookSourceEventDatasourceDeletedType:
    class Meta:
        name = "webhookSourceEventDatasourceDeletedType"
@dataclass
class WebhookSourceEventDatasourceRefreshFailedType:
    class Meta:
        name = "webhookSourceEventDatasourceRefreshFailedType"
@dataclass
class WebhookSourceEventDatasourceRefreshStartedType:
    class Meta:
        name = "webhookSourceEventDatasourceRefreshStartedType"
@dataclass
class WebhookSourceEventDatasourceRefreshSucceededType:
    class Meta:
        name = "webhookSourceEventDatasourceRefreshSucceededType"
@dataclass
class WebhookSourceEventDatasourceUpdatedType:
    class Meta:
        name = "webhookSourceEventDatasourceUpdatedType"
@dataclass
class WebhookSourceEventFlowCompletedType:
    class Meta:
        name = "webhookSourceEventFlowCompletedType"
@dataclass
class WebhookSourceEventLabelCreatedType:
    class Meta:
        name = "webhookSourceEventLabelCreatedType"
@dataclass
class WebhookSourceEventLabelDeletedType:
    class Meta:
        name = "webhookSourceEventLabelDeletedType"
@dataclass
class WebhookSourceEventLabelUpdatedType:
    class Meta:
        name = "webhookSourceEventLabelUpdatedType"
@dataclass
class WebhookSourceEventViewDeletedType:
    class Meta:
        name = "webhookSourceEventViewDeletedType"
@dataclass
class WebhookSourceEventWorkbookCreatedType:
    class Meta:
        name = "webhookSourceEventWorkbookCreatedType"
@dataclass
class WebhookSourceEventWorkbookDeletedType:
    class Meta:
        name = "webhookSourceEventWorkbookDeletedType"
@dataclass
class WebhookSourceEventWorkbookRefreshFailedType:
    class Meta:
        name = "webhookSourceEventWorkbookRefreshFailedType"
@dataclass
class WebhookSourceEventWorkbookRefreshStartedType:
    class Meta:
        name = "webhookSourceEventWorkbookRefreshStartedType"
@dataclass
class WebhookSourceEventWorkbookRefreshSucceededType:
    class Meta:
        name = "webhookSourceEventWorkbookRefreshSucceededType"
@dataclass
class WebhookSourceEventWorkbookUpdatedType:
    class Meta:
        name = "webhookSourceEventWorkbookUpdatedType"
@dataclass
class WebhookTestResultType:
    class Meta:
        name = "webhookTestResultType"

    body: Optional[str] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    status: Optional[int] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
@dataclass
class BackgroundJobType:
    class Meta:
        name = "backgroundJobType"

    created_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "createdAt",
            "type": "Attribute",
        }
    )
    ended_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "endedAt",
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    job_type: Optional[str] = field(
        default=None,
        metadata={
            "name": "jobType",
            "type": "Attribute",
        }
    )
    priority: Optional[int] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    started_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "startedAt",
            "type": "Attribute",
        }
    )
    status: Optional[BackgroundJobTypeStatus] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    subtitle: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    title: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
@dataclass
class CapabilityType:
    class Meta:
        name = "capabilityType"

    mode: Optional[CapabilityTypeMode] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "required": True,
        }
    )
    name: Optional[CapabilityTypeName] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "required": True,
        }
    )
@dataclass
class ConnectedApplicationType:
    class Meta:
        name = "connectedApplicationType"

    project_ids: Optional[ConnectedApplicationProjectListType] = field(
        default=None,
        metadata={
            "name": "projectIds",
            "type": "Element",
            "namespace": "",
        }
    )
    secret: list[ConnectedApplicationSecretType] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
            "max_occurs": 2,
        }
    )
    client_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "clientId",
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    created_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "createdAt",
            "type": "Attribute",
        }
    )
    domain_safelist: Optional[str] = field(
        default=None,
        metadata={
            "name": "domainSafelist",
            "type": "Attribute",
        }
    )
    enabled: Optional[bool] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    project_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "projectId",
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    unrestricted_embedding: Optional[bool] = field(
        default=None,
        metadata={
            "name": "unrestrictedEmbedding",
            "type": "Attribute",
        }
    )
@dataclass
class ContentActionType:
    class Meta:
        name = "contentActionType"

    action: Optional[ContentActionTypeAction] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "required": True,
        }
    )
@dataclass
class ContentListType:
    class Meta:
        name = "contentListType"

    content: list[ContentTypeAndIdType] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
            "min_occurs": 1,
        }
    )
@dataclass
class DataAccelerationInfoType:
    class Meta:
        name = "dataAccelerationInfoType"

    accelerate_now: bool = field(
        default=False,
        metadata={
            "name": "accelerateNow",
            "type": "Attribute",
        }
    )
    acceleration_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "accelerationEnabled",
            "type": "Attribute",
        }
    )
    acceleration_status: Optional[AccelerationStatusType] = field(
        default=None,
        metadata={
            "name": "accelerationStatus",
            "type": "Attribute",
        }
    )
    last_updated_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "lastUpdatedAt",
            "type": "Attribute",
        }
    )
@dataclass
class DataAlertUpdateStatusType:
    class Meta:
        name = "dataAlertUpdateStatusType"

    error: Optional[ErrorType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    success: Optional[bool] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
@dataclass
class DataAlertsRecipientListType:
    class Meta:
        name = "dataAlertsRecipientListType"

    recipient: list[DataAlertsRecipientType] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class DataQualityTriggerListType:
    class Meta:
        name = "dataQualityTriggerListType"

    data_quality_trigger: list[DataQualityTriggerType] = field(
        default_factory=list,
        metadata={
            "name": "dataQualityTrigger",
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class DataUpdateConstConditionType:
    class Meta:
        name = "dataUpdateConstConditionType"

    type_value: Optional[DataUpdateConstConditionTypeType] = field(
        default=None,
        metadata={
            "name": "type",
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    v: Optional[str] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
@dataclass
class DatabaseAnchorRequestType:
    class Meta:
        name = "databaseAnchorRequestType"

    connection_params: Optional[ConnectionParamsForAnchorType] = field(
        default=None,
        metadata={
            "name": "connectionParams",
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    datasource_formatted_name: Optional[str] = field(
        default=None,
        metadata={
            "name": "datasourceFormattedName",
            "type": "Attribute",
        }
    )
    datasource_name: Optional[str] = field(
        default=None,
        metadata={
            "name": "datasourceName",
            "type": "Attribute",
        }
    )
    is_archived: Optional[bool] = field(
        default=None,
        metadata={
            "name": "isArchived",
            "type": "Attribute",
        }
    )
@dataclass
class DegradationListType:
    class Meta:
        name = "degradationListType"

    downgraded_feature: list[DegradationType] = field(
        default_factory=list,
        metadata={
            "name": "downgradedFeature",
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class DomainDirectiveListType:
    class Meta:
        name = "domainDirectiveListType"

    domain: list[DomainDirectiveType] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class ExtensionsSiteSettingsType:
    class Meta:
        name = "extensionsSiteSettingsType"

    safe_list: list[ExtensionsSafeListEntry] = field(
        default_factory=list,
        metadata={
            "name": "safeList",
            "type": "Element",
            "namespace": "",
        }
    )
    extensions_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "extensionsEnabled",
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    use_default_setting: Optional[bool] = field(
        default=None,
        metadata={
            "name": "useDefaultSetting",
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
@dataclass
class ExternalAuthorizationServerListType:
    class Meta:
        name = "externalAuthorizationServerListType"

    external_authorization_server: list[ExternalAuthorizationServerType] = field(
        default_factory=list,
        metadata={
            "name": "externalAuthorizationServer",
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class FavoriteOrderingType:
    class Meta:
        name = "favoriteOrderingType"

    favorite_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "favoriteId",
            "type": "Attribute",
            "required": True,
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    favorite_id_move_after: Optional[str] = field(
        default=None,
        metadata={
            "name": "favoriteIdMoveAfter",
            "type": "Attribute",
            "required": True,
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    favorite_type: Optional[FavoriteTypeType] = field(
        default=None,
        metadata={
            "name": "favoriteType",
            "type": "Attribute",
            "required": True,
        }
    )
    favorite_type_move_after: Optional[FavoriteTypeType] = field(
        default=None,
        metadata={
            "name": "favoriteTypeMoveAfter",
            "type": "Attribute",
            "required": True,
        }
    )
@dataclass
class FlowOutputStepListType:
    class Meta:
        name = "flowOutputStepListType"

    flow_output_step: list[FlowOutputStepType] = field(
        default_factory=list,
        metadata={
            "name": "flowOutputStep",
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class FlowParameterAnyDomainType(FlowParameterDomainType):
    class Meta:
        name = "flowParameterAnyDomainType"
@dataclass
class FlowParameterBinaryDomainType(FlowParameterDomainType):
    class Meta:
        name = "flowParameterBinaryDomainType"
@dataclass
class FlowParameterListDomainType(FlowParameterDomainType):
    class Meta:
        name = "flowParameterListDomainType"

    values: Optional[FlowParameterListValueListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
@dataclass
class FlowParameterRangeDomainType(FlowParameterDomainType):
    class Meta:
        name = "flowParameterRangeDomainType"

    date_value_increment: Optional[str] = field(
        default=None,
        metadata={
            "name": "dateValueIncrement",
            "type": "Attribute",
        }
    )
    max_value: Optional[str] = field(
        default=None,
        metadata={
            "name": "maxValue",
            "type": "Attribute",
        }
    )
    min_value: Optional[str] = field(
        default=None,
        metadata={
            "name": "minValue",
            "type": "Attribute",
        }
    )
    step_size: Optional[str] = field(
        default=None,
        metadata={
            "name": "stepSize",
            "type": "Attribute",
        }
    )
@dataclass
class FlowParameterRunListType:
    class Meta:
        name = "flowParameterRunListType"

    parameter_runs: list[FlowParameterRunType] = field(
        default_factory=list,
        metadata={
            "name": "parameterRuns",
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class FlowParameterSystemDomainType(FlowParameterDomainType):
    class Meta:
        name = "flowParameterSystemDomainType"
@dataclass
class FlowParameterType:
    class Meta:
        name = "flowParameterType"

    domain: Optional[FlowParameterDomainType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    description: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    is_required: Optional[bool] = field(
        default=None,
        metadata={
            "name": "isRequired",
            "type": "Attribute",
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    type_value: Optional[str] = field(
        default=None,
        metadata={
            "name": "type",
            "type": "Attribute",
        }
    )
    value: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
@dataclass
class FlowRunSpecType:
    class Meta:
        name = "flowRunSpecType"

    flow_output_steps: Optional["FlowRunSpecType.FlowOutputSteps"] = field(
        default=None,
        metadata={
            "name": "flowOutputSteps",
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    flow_parameter_specs: Optional["FlowRunSpecType.FlowParameterSpecs"] = field(
        default=None,
        metadata={
            "name": "flowParameterSpecs",
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    flow_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "flowId",
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    run_mode: Optional[FlowRunSpecTypeRunMode] = field(
        default=None,
        metadata={
            "name": "runMode",
            "type": "Attribute",
        }
    )

    @dataclass
    class FlowOutputSteps:
        flow_output_step: list[FlowOutputStepType] = field(
            default_factory=list,
            metadata={
                "name": "flowOutputStep",
                "type": "Element",
                "namespace": "",
                "min_occurs": 1,
            }
        )

    @dataclass
    class FlowParameterSpecs:
        flow_parameter_spec: list[FlowParameterSpecType] = field(
            default_factory=list,
            metadata={
                "name": "flowParameterSpec",
                "type": "Element",
                "namespace": "",
                "min_occurs": 1,
            }
        )
@dataclass
class FreshEveryScheduleType:
    class Meta:
        name = "freshEveryScheduleType"

    frequency: Optional[FreshEveryScheduleTypeFrequency] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    value: Optional[int] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
@dataclass
class ImportDirectiveType:
    class Meta:
        name = "importDirectiveType"

    domain_name: Optional[str] = field(
        default=None,
        metadata={
            "name": "domainName",
            "type": "Attribute",
            "required": True,
        }
    )
    grant_license_mode: Optional[ImportDirectiveTypeGrantLicenseMode] = field(
        default=None,
        metadata={
            "name": "grantLicenseMode",
            "type": "Attribute",
            "required": True,
        }
    )
    site_role: Optional[SiteRoleType] = field(
        default=None,
        metadata={
            "name": "siteRole",
            "type": "Attribute",
            "required": True,
        }
    )
    source: Optional[ImportSourceType] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "required": True,
        }
    )
@dataclass
class IntervalType:
    class Meta:
        name = "intervalType"

    hours: Optional[IntervalTypeHours] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    minutes: Optional[IntervalTypeMinutes] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    month_day: Optional[IntervalTypeValue] = field(
        default=None,
        metadata={
            "name": "monthDay",
            "type": "Attribute",
        }
    )
    week_day: Optional[IntervalTypeWeekDay] = field(
        default=None,
        metadata={
            "name": "weekDay",
            "type": "Attribute",
        }
    )
@dataclass
class LabelCategoryListType:
    class Meta:
        name = "labelCategoryListType"

    label_category: list[LabelCategoryType] = field(
        default_factory=list,
        metadata={
            "name": "labelCategory",
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class LinkUserExecutionContextType:
    class Meta:
        name = "linkUserExecutionContextType"

    content_url: Optional[str] = field(
        default=None,
        metadata={
            "name": "contentUrl",
            "type": "Attribute",
            "required": True,
        }
    )
    operation_type: Optional[LinkUserOperationType] = field(
        default=None,
        metadata={
            "name": "operationType",
            "type": "Attribute",
        }
    )
    site_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "siteId",
            "type": "Attribute",
            "required": True,
        }
    )
    site_name: Optional[str] = field(
        default=None,
        metadata={
            "name": "siteName",
            "type": "Attribute",
            "required": True,
        }
    )
    tenant_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "tenantId",
            "type": "Attribute",
            "required": True,
        }
    )
    user_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "userId",
            "type": "Attribute",
            "required": True,
        }
    )
    user_name: Optional[str] = field(
        default=None,
        metadata={
            "name": "userName",
            "type": "Attribute",
            "required": True,
        }
    )
    user_sync_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "userSyncId",
            "type": "Attribute",
            "required": True,
        }
    )
@dataclass
class LinkUserSiteRoleType:
    class Meta:
        name = "linkUserSiteRoleType"

    idp: Optional[LinkUserIdptype] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    site_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "siteId",
            "type": "Attribute",
            "required": True,
        }
    )
    site_role: Optional[SiteRoleType] = field(
        default=None,
        metadata={
            "name": "siteRole",
            "type": "Attribute",
        }
    )
@dataclass
class LinkedTaskJobType:
    class Meta:
        name = "linkedTaskJobType"

    created_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "createdAt",
            "type": "Attribute",
        }
    )
    ended_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "endedAt",
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    linked_task_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "linkedTaskId",
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    started_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "startedAt",
            "type": "Attribute",
        }
    )
    status: Optional[LinkedTaskJobTypeStatus] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
@dataclass
class LocationType:
    class Meta:
        name = "locationType"

    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    type_value: Optional[LocationTypeType] = field(
        default=None,
        metadata={
            "name": "type",
            "type": "Attribute",
        }
    )
@dataclass
class NotificationPreferenceUpdateStatusType:
    class Meta:
        name = "notificationPreferenceUpdateStatusType"

    error: Optional[ErrorType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    status: Optional[NotificationPreferenceUpdateStatusTypeStatus] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    user_notifications_preference: Optional[UserNotificationsPreferenceType] = field(
        default=None,
        metadata={
            "name": "userNotificationsPreference",
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
@dataclass
class ParentType:
    class Meta:
        name = "parentType"

    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "required": True,
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    type_value: Optional[ParentTypeType] = field(
        default=None,
        metadata={
            "name": "type",
            "type": "Attribute",
            "required": True,
        }
    )
@dataclass
class PersonalAccessTokenListType:
    class Meta:
        name = "personalAccessTokenListType"

    personal_access_token: list[PersonalAccessTokenType] = field(
        default_factory=list,
        metadata={
            "name": "personalAccessToken",
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class PolicyType:
    class Meta:
        name = "policyType"

    value_list: list[str] = field(
        default_factory=list,
        metadata={
            "name": "valueList",
            "type": "Element",
            "namespace": "",
            "min_occurs": 1,
        }
    )
    enabled: Optional[bool] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "required": True,
        }
    )
    severity: Optional[SeverityLevelType] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "required": True,
        }
    )
@dataclass
class PublishToSalesforceBatchType:
    class Meta:
        name = "publishToSalesforceBatchType"

    publish_to_salesforce_info: list[PublishToSalesforceInfoType] = field(
        default_factory=list,
        metadata={
            "name": "publishToSalesforceInfo",
            "type": "Element",
            "namespace": "",
            "min_occurs": 1,
        }
    )
    has_errors: Optional[bool] = field(
        default=None,
        metadata={
            "name": "hasErrors",
            "type": "Attribute",
        }
    )
    salesforce_app_url: Optional[str] = field(
        default=None,
        metadata={
            "name": "salesforceAppUrl",
            "type": "Attribute",
        }
    )
@dataclass
class ResourceList:
    class Meta:
        name = "resourceList"

    resource: list[ContentTypeAndIdType] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
            "min_occurs": 1,
        }
    )
@dataclass
class SalesforceAppListType:
    class Meta:
        name = "salesforceAppListType"

    salesforce_app: list[SalesforceAppType] = field(
        default_factory=list,
        metadata={
            "name": "salesforceApp",
            "type": "Element",
            "namespace": "",
            "min_occurs": 1,
        }
    )
    next_page_token: Optional[str] = field(
        default=None,
        metadata={
            "name": "nextPageToken",
            "type": "Attribute",
        }
    )
@dataclass
class ServerInfo:
    class Meta:
        name = "serverInfo"

    platform: Optional[str] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    prep_conductor_version: Optional[str] = field(
        default=None,
        metadata={
            "name": "prepConductorVersion",
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    product_version: Optional[ProductVersion] = field(
        default=None,
        metadata={
            "name": "productVersion",
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    rest_api_version: Optional[str] = field(
        default=None,
        metadata={
            "name": "restApiVersion",
            "type": "Element",
            "namespace": "",
            "required": True,
            "pattern": r'[0-9]{5}.[0-9]{2}.[0-9]{4}.[0-9]{4}',
        }
    )
    server_settings: Optional[ServerSettings] = field(
        default=None,
        metadata={
            "name": "serverSettings",
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
@dataclass
class ServiceTokenListType:
    class Meta:
        name = "serviceTokenListType"

    service_token: list[ServiceTokenType] = field(
        default_factory=list,
        metadata={
            "name": "serviceToken",
            "type": "Element",
            "namespace": "",
        }
    )
    has_errors: Optional[bool] = field(
        default=None,
        metadata={
            "name": "hasErrors",
            "type": "Attribute",
        }
    )
@dataclass
class SiteOidcconfigurationListType:
    class Meta:
        name = "siteOIDCConfigurationListType"

    site_oidcconfiguration: list[SiteOidcconfigurationType] = field(
        default_factory=list,
        metadata={
            "name": "siteOIDCConfiguration",
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class SiteType:
    class Meta:
        name = "siteType"

    settings: list[EmbeddingSettingsType] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    usage: Optional["SiteType.Usage"] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    admin_insights_publish_frequency: Optional[int] = field(
        default=None,
        metadata={
            "name": "adminInsightsPublishFrequency",
            "type": "Attribute",
        }
    )
    admin_mode: Optional[SiteTypeAdminMode] = field(
        default=None,
        metadata={
            "name": "adminMode",
            "type": "Attribute",
        }
    )
    allow_subscription_attachments: Optional[bool] = field(
        default=None,
        metadata={
            "name": "allowSubscriptionAttachments",
            "type": "Attribute",
        }
    )
    ask_data_mode: Optional[SiteTypeAskDataMode] = field(
        default=None,
        metadata={
            "name": "askDataMode",
            "type": "Attribute",
        }
    )
    attribute_capture_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "attributeCaptureEnabled",
            "type": "Attribute",
        }
    )
    authoring_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "authoringEnabled",
            "type": "Attribute",
        }
    )
    auto_extract_refresh_monitoring_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "autoExtractRefreshMonitoringEnabled",
            "type": "Attribute",
        }
    )
    auto_flow_run_monitoring_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "autoFlowRunMonitoringEnabled",
            "type": "Attribute",
        }
    )
    auto_suspend_refresh_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "autoSuspendRefreshEnabled",
            "type": "Attribute",
        }
    )
    auto_suspend_refresh_inactivity_window: Optional[int] = field(
        default=None,
        metadata={
            "name": "autoSuspendRefreshInactivityWindow",
            "type": "Attribute",
        }
    )
    cache_warmup_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "cacheWarmupEnabled",
            "type": "Attribute",
        }
    )
    catalog_obfuscation_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "catalogObfuscationEnabled",
            "type": "Attribute",
        }
    )
    cataloging_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "catalogingEnabled",
            "type": "Attribute",
        }
    )
    cmek_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "cmekEnabled",
            "type": "Attribute",
        }
    )
    commenting_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "commentingEnabled",
            "type": "Attribute",
        }
    )
    commenting_mentions_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "commentingMentionsEnabled",
            "type": "Attribute",
        }
    )
    content_migration_tool_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "contentMigrationToolEnabled",
            "type": "Attribute",
        }
    )
    content_url: Optional[str] = field(
        default=None,
        metadata={
            "name": "contentUrl",
            "type": "Attribute",
        }
    )
    custom_subscription_email: Optional[str] = field(
        default=None,
        metadata={
            "name": "customSubscriptionEmail",
            "type": "Attribute",
        }
    )
    custom_subscription_email_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "customSubscriptionEmailEnabled",
            "type": "Attribute",
        }
    )
    custom_subscription_footer: Optional[str] = field(
        default=None,
        metadata={
            "name": "customSubscriptionFooter",
            "type": "Attribute",
        }
    )
    custom_subscription_footer_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "customSubscriptionFooterEnabled",
            "type": "Attribute",
        }
    )
    data_acceleration_mode: Optional[str] = field(
        default=None,
        metadata={
            "name": "dataAccelerationMode",
            "type": "Attribute",
        }
    )
    data_alerts_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "dataAlertsEnabled",
            "type": "Attribute",
        }
    )
    data_orientation_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "dataOrientationEnabled",
            "type": "Attribute",
        }
    )
    data_story_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "dataStoryEnabled",
            "type": "Attribute",
        }
    )
    derived_permissions_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "derivedPermissionsEnabled",
            "type": "Attribute",
        }
    )
    disable_subscriptions: Optional[bool] = field(
        default=None,
        metadata={
            "name": "disableSubscriptions",
            "type": "Attribute",
        }
    )
    dqw_subscriptions_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "dqwSubscriptionsEnabled",
            "type": "Attribute",
        }
    )
    eas_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "easEnabled",
            "type": "Attribute",
        }
    )
    editing_flows_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "editingFlowsEnabled",
            "type": "Attribute",
        }
    )
    einstein_in_flow_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "einsteinInFlowEnabled",
            "type": "Attribute",
        }
    )
    explain_data_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "explainDataEnabled",
            "type": "Attribute",
        }
    )
    extract_encryption_mode: Optional[str] = field(
        default=None,
        metadata={
            "name": "extractEncryptionMode",
            "type": "Attribute",
        }
    )
    flow_auto_save_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "flowAutoSaveEnabled",
            "type": "Attribute",
        }
    )
    flow_output_subscriptions_data_as_email_attachment_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "flowOutputSubscriptionsDataAsEmailAttachmentEnabled",
            "type": "Attribute",
        }
    )
    flow_output_subscriptions_data_in_email_body_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "flowOutputSubscriptionsDataInEmailBodyEnabled",
            "type": "Attribute",
        }
    )
    flow_output_subscriptions_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "flowOutputSubscriptionsEnabled",
            "type": "Attribute",
        }
    )
    flow_parameters_any_type_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "flowParametersAnyTypeEnabled",
            "type": "Attribute",
        }
    )
    flow_parameters_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "flowParametersEnabled",
            "type": "Attribute",
        }
    )
    flow_parameters_system_type_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "flowParametersSystemTypeEnabled",
            "type": "Attribute",
        }
    )
    flows_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "flowsEnabled",
            "type": "Attribute",
        }
    )
    generative_ai_data_catalog: Optional[bool] = field(
        default=None,
        metadata={
            "name": "generativeAiDataCatalog",
            "type": "Attribute",
        }
    )
    generative_ai_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "generativeAiEnabled",
            "type": "Attribute",
        }
    )
    generative_ai_prep: Optional[bool] = field(
        default=None,
        metadata={
            "name": "generativeAiPrep",
            "type": "Attribute",
        }
    )
    generative_ai_pulse: Optional[bool] = field(
        default=None,
        metadata={
            "name": "generativeAiPulse",
            "type": "Attribute",
        }
    )
    generative_ai_web_authoring: Optional[bool] = field(
        default=None,
        metadata={
            "name": "generativeAiWebAuthoring",
            "type": "Attribute",
        }
    )
    group_assertions_connected_apps_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "groupAssertionsConnectedAppsEnabled",
            "type": "Attribute",
        }
    )
    group_assertions_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "groupAssertionsEnabled",
            "type": "Attribute",
        }
    )
    group_assertions_oidcenabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "groupAssertionsOIDCEnabled",
            "type": "Attribute",
        }
    )
    group_assertions_samlenabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "groupAssertionsSAMLEnabled",
            "type": "Attribute",
        }
    )
    group_sets_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "groupSetsEnabled",
            "type": "Attribute",
        }
    )
    guest_access_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "guestAccessEnabled",
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    linked_task_run_now_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "linkedTaskRunNowEnabled",
            "type": "Attribute",
        }
    )
    linked_task_scheduling_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "linkedTaskSchedulingEnabled",
            "type": "Attribute",
        }
    )
    login_based_license_management_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "loginBasedLicenseManagementEnabled",
            "type": "Attribute",
        }
    )
    max_service_token_limit_per_user: Optional[int] = field(
        default=None,
        metadata={
            "name": "maxServiceTokenLimitPerUser",
            "type": "Attribute",
        }
    )
    metrics_content_type_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "metricsContentTypeEnabled",
            "type": "Attribute",
        }
    )
    mfa_enforcement_exemption: Optional[bool] = field(
        default=None,
        metadata={
            "name": "mfaEnforcementExemption",
            "type": "Attribute",
        }
    )
    mobile_biometrics_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "mobileBiometricsEnabled",
            "type": "Attribute",
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    named_sharing_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "namedSharingEnabled",
            "type": "Attribute",
        }
    )
    notify_site_admins_on_throttle: Optional[bool] = field(
        default=None,
        metadata={
            "name": "notifySiteAdminsOnThrottle",
            "type": "Attribute",
        }
    )
    personal_space_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "personalSpaceEnabled",
            "type": "Attribute",
        }
    )
    personal_space_storage_quota: Optional[str] = field(
        default=None,
        metadata={
            "name": "personalSpaceStorageQuota",
            "type": "Attribute",
        }
    )
    publish_to_salesforce_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "publishToSalesforceEnabled",
            "type": "Attribute",
        }
    )
    pulse_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "pulseEnabled",
            "type": "Attribute",
        }
    )
    pulse_group_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "pulseGroupId",
            "type": "Attribute",
        }
    )
    pulse_personalized_ranking_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "pulsePersonalizedRankingEnabled",
            "type": "Attribute",
        }
    )
    refresh_token_absolute_expiry: Optional[int] = field(
        default=None,
        metadata={
            "name": "refreshTokenAbsoluteExpiry",
            "type": "Attribute",
        }
    )
    request_access_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "requestAccessEnabled",
            "type": "Attribute",
        }
    )
    revision_history_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "revisionHistoryEnabled",
            "type": "Attribute",
        }
    )
    revision_limit: Optional[Union[int, RevisionLimitTypeValue]] = field(
        default=None,
        metadata={
            "name": "revisionLimit",
            "type": "Attribute",
            "min_inclusive": 2,
            "max_inclusive": 10000,
        }
    )
    run_now_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "runNowEnabled",
            "type": "Attribute",
        }
    )
    scheduling_flows_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "schedulingFlowsEnabled",
            "type": "Attribute",
        }
    )
    self_service_schedule_for_refresh_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "selfServiceScheduleForRefreshEnabled",
            "type": "Attribute",
        }
    )
    sheet_image_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "sheetImageEnabled",
            "type": "Attribute",
        }
    )
    site_invite_email_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "siteInviteEmailEnabled",
            "type": "Attribute",
        }
    )
    site_prompted_login_flow_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "sitePromptedLoginFlowEnabled",
            "type": "Attribute",
        }
    )
    state: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    storage_quota: Optional[str] = field(
        default=None,
        metadata={
            "name": "storageQuota",
            "type": "Attribute",
        }
    )
    subscribe_others_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "subscribeOthersEnabled",
            "type": "Attribute",
        }
    )
    tag_limit: Optional[int] = field(
        default=None,
        metadata={
            "name": "tagLimit",
            "type": "Attribute",
        }
    )
    tier_creator_capacity: Optional[int] = field(
        default=None,
        metadata={
            "name": "tierCreatorCapacity",
            "type": "Attribute",
            "min_inclusive": 0,
        }
    )
    tier_explorer_capacity: Optional[int] = field(
        default=None,
        metadata={
            "name": "tierExplorerCapacity",
            "type": "Attribute",
            "min_inclusive": 0,
        }
    )
    tier_viewer_capacity: Optional[int] = field(
        default=None,
        metadata={
            "name": "tierViewerCapacity",
            "type": "Attribute",
            "min_inclusive": 0,
        }
    )
    time_zone: Optional[str] = field(
        default=None,
        metadata={
            "name": "timeZone",
            "type": "Attribute",
        }
    )
    use_default_time_zone: Optional[bool] = field(
        default=None,
        metadata={
            "name": "useDefaultTimeZone",
            "type": "Attribute",
        }
    )
    user_quota: Optional[str] = field(
        default=None,
        metadata={
            "name": "userQuota",
            "type": "Attribute",
        }
    )
    user_visibility_mode: Optional[str] = field(
        default=None,
        metadata={
            "name": "userVisibilityMode",
            "type": "Attribute",
        }
    )
    web_extraction_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "webExtractionEnabled",
            "type": "Attribute",
        }
    )
    web_zone_content_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "webZoneContentEnabled",
            "type": "Attribute",
        }
    )
    workflow_extension_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "workflowExtensionEnabled",
            "type": "Attribute",
        }
    )

    @dataclass
    class Usage:
        num_creators: Optional[int] = field(
            default=None,
            metadata={
                "name": "numCreators",
                "type": "Attribute",
            }
        )
        num_explorers: Optional[int] = field(
            default=None,
            metadata={
                "name": "numExplorers",
                "type": "Attribute",
            }
        )
        num_users: Optional[int] = field(
            default=None,
            metadata={
                "name": "numUsers",
                "type": "Attribute",
                "required": True,
            }
        )
        num_viewers: Optional[int] = field(
            default=None,
            metadata={
                "name": "numViewers",
                "type": "Attribute",
            }
        )
        storage: Optional[int] = field(
            default=None,
            metadata={
                "type": "Attribute",
                "required": True,
            }
        )
@dataclass
class StatusNoteType:
    class Meta:
        name = "statusNoteType"

    text: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    type_value: Optional[StatusNoteTypeType] = field(
        default=None,
        metadata={
            "name": "type",
            "type": "Attribute",
            "required": True,
        }
    )
    value: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
@dataclass
class SubscriptionContentType:
    class Meta:
        name = "subscriptionContentType"

    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "required": True,
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    name: Optional[object] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    send_if_view_empty: Optional[bool] = field(
        default=None,
        metadata={
            "name": "sendIfViewEmpty",
            "type": "Attribute",
        }
    )
    type_value: Optional[SubscriptionContentTypeType] = field(
        default=None,
        metadata={
            "name": "type",
            "type": "Attribute",
            "required": True,
        }
    )
@dataclass
class SuggestionListType:
    class Meta:
        name = "suggestionListType"

    suggestion: list[SuggestionType] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class TableAnchorListType:
    class Meta:
        name = "tableAnchorListType"

    table_anchor: list[TableAnchorType] = field(
        default_factory=list,
        metadata={
            "name": "tableAnchor",
            "type": "Element",
            "namespace": "",
            "min_occurs": 1,
        }
    )
@dataclass
class TagListType:
    class Meta:
        name = "tagListType"

    tag: list[TagType] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class TasSiteOauthClientListType:
    class Meta:
        name = "tasSiteOAuthClientListType"

    tas_site_oauth_client: list[TasSiteOauthClientType] = field(
        default_factory=list,
        metadata={
            "name": "tasSiteOAuthClient",
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class UserNotificationsPreferenceListType:
    class Meta:
        name = "userNotificationsPreferenceListType"

    user_notifications_preference: list[UserNotificationsPreferenceType] = field(
        default_factory=list,
        metadata={
            "name": "userNotificationsPreference",
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class UserType:
    class Meta:
        name = "userType"

    domain: Optional[DomainDirectiveType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    auth_setting: Optional[SiteUserAuthSettingType] = field(
        default=None,
        metadata={
            "name": "authSetting",
            "type": "Attribute",
        }
    )
    content_admin: Optional[bool] = field(
        default=None,
        metadata={
            "name": "contentAdmin",
            "type": "Attribute",
        }
    )
    email: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    external_auth_user_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "externalAuthUserId",
            "type": "Attribute",
        }
    )
    full_name: Optional[str] = field(
        default=None,
        metadata={
            "name": "fullName",
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    identifier: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    identity_pool_name: Optional[str] = field(
        default=None,
        metadata={
            "name": "identityPoolName",
            "type": "Attribute",
        }
    )
    identity_pool_uuid: Optional[str] = field(
        default=None,
        metadata={
            "name": "identityPoolUuid",
            "type": "Attribute",
        }
    )
    identity_uuid: Optional[str] = field(
        default=None,
        metadata={
            "name": "identityUuid",
            "type": "Attribute",
        }
    )
    language: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    last_login: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "lastLogin",
            "type": "Attribute",
        }
    )
    locale: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    password: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    publish: Optional[bool] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    role: Optional[LicensingRoleType] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    site_role: Optional[SiteRoleType] = field(
        default=None,
        metadata={
            "name": "siteRole",
            "type": "Attribute",
        }
    )
    suppress_getting_started: Optional[bool] = field(
        default=None,
        metadata={
            "name": "suppressGettingStarted",
            "type": "Attribute",
        }
    )
@dataclass
class VirtualConnectionConnectionsType:
    class Meta:
        name = "virtualConnectionConnectionsType"

    connection: list[VirtualConnectionSourceConnectionType] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class WarningListType:
    class Meta:
        name = "warningListType"

    warning: list[WarningType] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class WebhookDestinationHttpType:
    class Meta:
        name = "webhookDestinationHttpType"

    method: Optional[WebhookDestinationHttpTypeMethod] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    url: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'https?://.+',
        }
    )
@dataclass
class WebhookSourceType:
    class Meta:
        name = "webhookSourceType"

    webhook_source_event_datasource_created: Optional[WebhookSourceEventDatasourceCreatedType] = field(
        default=None,
        metadata={
            "name": "webhook-source-event-datasource-created",
            "type": "Element",
            "namespace": "",
        }
    )
    webhook_source_event_datasource_deleted: Optional[WebhookSourceEventDatasourceDeletedType] = field(
        default=None,
        metadata={
            "name": "webhook-source-event-datasource-deleted",
            "type": "Element",
            "namespace": "",
        }
    )
    webhook_source_event_datasource_refresh_failed: Optional[WebhookSourceEventDatasourceRefreshFailedType] = field(
        default=None,
        metadata={
            "name": "webhook-source-event-datasource-refresh-failed",
            "type": "Element",
            "namespace": "",
        }
    )
    webhook_source_event_datasource_refresh_started: Optional[WebhookSourceEventDatasourceRefreshStartedType] = field(
        default=None,
        metadata={
            "name": "webhook-source-event-datasource-refresh-started",
            "type": "Element",
            "namespace": "",
        }
    )
    webhook_source_event_datasource_refresh_succeeded: Optional[WebhookSourceEventDatasourceRefreshSucceededType] = field(
        default=None,
        metadata={
            "name": "webhook-source-event-datasource-refresh-succeeded",
            "type": "Element",
            "namespace": "",
        }
    )
    webhook_source_event_datasource_updated: Optional[WebhookSourceEventDatasourceUpdatedType] = field(
        default=None,
        metadata={
            "name": "webhook-source-event-datasource-updated",
            "type": "Element",
            "namespace": "",
        }
    )
    webhook_source_event_flow_completed: Optional[WebhookSourceEventFlowCompletedType] = field(
        default=None,
        metadata={
            "name": "webhook-source-event-flow-completed",
            "type": "Element",
            "namespace": "",
        }
    )
    webhook_source_event_label_created: Optional[WebhookSourceEventLabelCreatedType] = field(
        default=None,
        metadata={
            "name": "webhook-source-event-label-created",
            "type": "Element",
            "namespace": "",
        }
    )
    webhook_source_event_label_deleted: Optional[WebhookSourceEventLabelDeletedType] = field(
        default=None,
        metadata={
            "name": "webhook-source-event-label-deleted",
            "type": "Element",
            "namespace": "",
        }
    )
    webhook_source_event_label_updated: Optional[WebhookSourceEventLabelUpdatedType] = field(
        default=None,
        metadata={
            "name": "webhook-source-event-label-updated",
            "type": "Element",
            "namespace": "",
        }
    )
    webhook_source_event_view_deleted: Optional[WebhookSourceEventViewDeletedType] = field(
        default=None,
        metadata={
            "name": "webhook-source-event-view-deleted",
            "type": "Element",
            "namespace": "",
        }
    )
    webhook_source_event_workbook_created: Optional[WebhookSourceEventWorkbookCreatedType] = field(
        default=None,
        metadata={
            "name": "webhook-source-event-workbook-created",
            "type": "Element",
            "namespace": "",
        }
    )
    webhook_source_event_workbook_deleted: Optional[WebhookSourceEventWorkbookDeletedType] = field(
        default=None,
        metadata={
            "name": "webhook-source-event-workbook-deleted",
            "type": "Element",
            "namespace": "",
        }
    )
    webhook_source_event_workbook_refresh_failed: Optional[WebhookSourceEventWorkbookRefreshFailedType] = field(
        default=None,
        metadata={
            "name": "webhook-source-event-workbook-refresh-failed",
            "type": "Element",
            "namespace": "",
        }
    )
    webhook_source_event_workbook_refresh_started: Optional[WebhookSourceEventWorkbookRefreshStartedType] = field(
        default=None,
        metadata={
            "name": "webhook-source-event-workbook-refresh-started",
            "type": "Element",
            "namespace": "",
        }
    )
    webhook_source_event_workbook_refresh_succeeded: Optional[WebhookSourceEventWorkbookRefreshSucceededType] = field(
        default=None,
        metadata={
            "name": "webhook-source-event-workbook-refresh-succeeded",
            "type": "Element",
            "namespace": "",
        }
    )
    webhook_source_event_workbook_updated: Optional[WebhookSourceEventWorkbookUpdatedType] = field(
        default=None,
        metadata={
            "name": "webhook-source-event-workbook-updated",
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class BackgroundJobListType:
    class Meta:
        name = "backgroundJobListType"

    background_job: list[BackgroundJobType] = field(
        default_factory=list,
        metadata={
            "name": "backgroundJob",
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class CollectionType:
    class Meta:
        name = "collectionType"

    owner: Optional[UserType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    created_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "createdAt",
            "type": "Attribute",
        }
    )
    description: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    permissioned_item_count: Optional[int] = field(
        default=None,
        metadata={
            "name": "permissionedItemCount",
            "type": "Attribute",
        }
    )
    total_item_count: Optional[int] = field(
        default=None,
        metadata={
            "name": "totalItemCount",
            "type": "Attribute",
        }
    )
    updated_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "updatedAt",
            "type": "Attribute",
        }
    )
    visibility: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
@dataclass
class ConnectedApplicationListType:
    class Meta:
        name = "connectedApplicationListType"

    connected_application: list[ConnectedApplicationType] = field(
        default_factory=list,
        metadata={
            "name": "connectedApplication",
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class ContentLocationRequestType:
    class Meta:
        name = "contentLocationRequestType"

    content_action: Optional[ContentActionType] = field(
        default=None,
        metadata={
            "name": "contentAction",
            "type": "Element",
            "namespace": "",
        }
    )
    location: Optional[LocationType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    resource_list: Optional[ResourceList] = field(
        default=None,
        metadata={
            "name": "resourceList",
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
@dataclass
class CustomViewAsUserDefaultViewResultType:
    class Meta:
        name = "customViewAsUserDefaultViewResultType"

    error: Optional[ErrorType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    user: Optional[UserType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    success: Optional[bool] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
@dataclass
class DataAlertCreateAlertType:
    class Meta:
        name = "dataAlertCreateAlertType"

    recipients: Optional[DataAlertsRecipientListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    alert_condition: Optional[DataAlertCreateAlertTypeAlertCondition] = field(
        default=None,
        metadata={
            "name": "alertCondition",
            "type": "Attribute",
        }
    )
    alert_threshold: Optional[str] = field(
        default=None,
        metadata={
            "name": "alertThreshold",
            "type": "Attribute",
        }
    )
    created_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "createdAt",
            "type": "Attribute",
        }
    )
    custom_view_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "customViewId",
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    device: Optional[DataAlertCreateAlertTypeDevice] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    frequency: Optional[DataAlertCreateAlertTypeFrequency] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    subject: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    updated_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "updatedAt",
            "type": "Attribute",
        }
    )
    view_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "viewId",
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    visibility: Optional[DataAlertCreateAlertTypeVisibility] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    worksheet_name: Optional[str] = field(
        default=None,
        metadata={
            "name": "worksheetName",
            "type": "Attribute",
        }
    )
@dataclass
class DataAlertUpdateStatusListType:
    class Meta:
        name = "dataAlertUpdateStatusListType"

    data_alert_update_status: list[DataAlertUpdateStatusType] = field(
        default_factory=list,
        metadata={
            "name": "dataAlertUpdateStatus",
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class DataQualityIndicatorType:
    class Meta:
        name = "dataQualityIndicatorType"

    owner: Optional[UserType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    site: Optional[SiteType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    active: Optional[bool] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    content_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "contentId",
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    content_type: Optional[str] = field(
        default=None,
        metadata={
            "name": "contentType",
            "type": "Attribute",
        }
    )
    created_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "createdAt",
            "type": "Attribute",
        }
    )
    elevated: Optional[bool] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    message: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    type_value: Optional[str] = field(
        default=None,
        metadata={
            "name": "type",
            "type": "Attribute",
        }
    )
    updated_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "updatedAt",
            "type": "Attribute",
        }
    )
    user_display_name: Optional[str] = field(
        default=None,
        metadata={
            "name": "userDisplayName",
            "type": "Attribute",
        }
    )
@dataclass
class DataQualityWarningType:
    class Meta:
        name = "dataQualityWarningType"

    owner: Optional[UserType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    site: Optional[SiteType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    content_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "contentId",
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    content_type: Optional[str] = field(
        default=None,
        metadata={
            "name": "contentType",
            "type": "Attribute",
        }
    )
    created_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "createdAt",
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    is_active: Optional[bool] = field(
        default=None,
        metadata={
            "name": "isActive",
            "type": "Attribute",
        }
    )
    is_severe: Optional[bool] = field(
        default=None,
        metadata={
            "name": "isSevere",
            "type": "Attribute",
        }
    )
    message: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    type_value: Optional[str] = field(
        default=None,
        metadata={
            "name": "type",
            "type": "Attribute",
        }
    )
    updated_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "updatedAt",
            "type": "Attribute",
        }
    )
    user_display_name: Optional[str] = field(
        default=None,
        metadata={
            "name": "userDisplayName",
            "type": "Attribute",
        }
    )
@dataclass
class DataUpdateConditionType:
    class Meta:
        name = "dataUpdateConditionType"

    const: Optional[DataUpdateConstConditionType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    source_col: Optional[str] = field(
        default=None,
        metadata={
            "name": "source-col",
            "type": "Element",
            "namespace": "",
        }
    )
    target_col: Optional[str] = field(
        default=None,
        metadata={
            "name": "target-col",
            "type": "Element",
            "namespace": "",
        }
    )
    args: list["DataUpdateConditionType"] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    op: Optional[str] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
@dataclass
class DatabaseAnchorResponseType:
    class Meta:
        name = "databaseAnchorResponseType"

    table_anchors: Optional[TableAnchorListType] = field(
        default=None,
        metadata={
            "name": "tableAnchors",
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    connection_name: Optional[str] = field(
        default=None,
        metadata={
            "name": "connectionName",
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
@dataclass
class DatabaseType:
    class Meta:
        name = "databaseType"

    certifier: Optional[UserType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    contact: Optional[UserType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    location: Optional[LocationType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    site: Optional[SiteType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    tags: Optional[TagListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    certification_note: Optional[str] = field(
        default=None,
        metadata={
            "name": "certificationNote",
            "type": "Attribute",
        }
    )
    connection_type: Optional[str] = field(
        default=None,
        metadata={
            "name": "connectionType",
            "type": "Attribute",
        }
    )
    connector_url: Optional[str] = field(
        default=None,
        metadata={
            "name": "connectorUrl",
            "type": "Attribute",
        }
    )
    content_permissions: Optional[DatabaseTypeContentPermissions] = field(
        default=None,
        metadata={
            "name": "contentPermissions",
            "type": "Attribute",
        }
    )
    description: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    file_extension: Optional[str] = field(
        default=None,
        metadata={
            "name": "fileExtension",
            "type": "Attribute",
        }
    )
    file_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "fileId",
            "type": "Attribute",
        }
    )
    file_path: Optional[str] = field(
        default=None,
        metadata={
            "name": "filePath",
            "type": "Attribute",
        }
    )
    host_name: Optional[str] = field(
        default=None,
        metadata={
            "name": "hostName",
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    is_certified: Optional[bool] = field(
        default=None,
        metadata={
            "name": "isCertified",
            "type": "Attribute",
        }
    )
    is_embedded: Optional[bool] = field(
        default=None,
        metadata={
            "name": "isEmbedded",
            "type": "Attribute",
        }
    )
    mime_type: Optional[str] = field(
        default=None,
        metadata={
            "name": "mimeType",
            "type": "Attribute",
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    port: Optional[int] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    provider: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    request_url: Optional[str] = field(
        default=None,
        metadata={
            "name": "requestUrl",
            "type": "Attribute",
        }
    )
    type_value: Optional[DatabaseTypeType] = field(
        default=None,
        metadata={
            "name": "type",
            "type": "Attribute",
        }
    )
@dataclass
class FavoriteOrderingListType:
    class Meta:
        name = "favoriteOrderingListType"

    favorite_ordering: list[FavoriteOrderingType] = field(
        default_factory=list,
        metadata={
            "name": "favoriteOrdering",
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class FlowParameterListType:
    class Meta:
        name = "flowParameterListType"

    parameter: list[FlowParameterType] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class FlowRunType:
    class Meta:
        name = "flowRunType"

    flow_parameter_runs: Optional[FlowParameterRunListType] = field(
        default=None,
        metadata={
            "name": "flowParameterRuns",
            "type": "Element",
            "namespace": "",
        }
    )
    background_job_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "backgroundJobId",
            "type": "Attribute",
        }
    )
    completed_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "completedAt",
            "type": "Attribute",
        }
    )
    flow_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "flowId",
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    progress: Optional[int] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    started_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "startedAt",
            "type": "Attribute",
        }
    )
    status: Optional[FlowRunTypeStatus] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
@dataclass
class FlowWarningsListContainerType:
    class Meta:
        name = "flowWarningsListContainerType"

    connection_warnings: Optional[WarningListType] = field(
        default=None,
        metadata={
            "name": "connectionWarnings",
            "type": "Element",
            "namespace": "",
        }
    )
    node_warnings: Optional[WarningListType] = field(
        default=None,
        metadata={
            "name": "nodeWarnings",
            "type": "Element",
            "namespace": "",
        }
    )
    run_mode: Optional[FlowWarningsListContainerTypeRunMode] = field(
        default=None,
        metadata={
            "name": "runMode",
            "type": "Attribute",
        }
    )
@dataclass
class FrequencyDetailsType:
    class Meta:
        name = "frequencyDetailsType"

    intervals: Optional["FrequencyDetailsType.Intervals"] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    end: Optional[XmlTime] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    start: Optional[XmlTime] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "required": True,
        }
    )

    @dataclass
    class Intervals:
        interval: list[IntervalType] = field(
            default_factory=list,
            metadata={
                "type": "Element",
                "namespace": "",
                "max_occurs": 7,
            }
        )
@dataclass
class FreshAtScheduleType:
    class Meta:
        name = "freshAtScheduleType"

    intervals: Optional["FreshAtScheduleType.Intervals"] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    frequency: Optional[FreshAtScheduleTypeFrequency] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "required": True,
        }
    )
    time: Optional[XmlTime] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "required": True,
        }
    )
    timezone: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )

    @dataclass
    class Intervals:
        interval: list[IntervalType] = field(
            default_factory=list,
            metadata={
                "type": "Element",
                "namespace": "",
                "max_occurs": 31,
            }
        )
@dataclass
class GroupType:
    class Meta:
        name = "groupType"

    domain: Optional[DomainDirectiveType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    import_value: Optional[ImportDirectiveType] = field(
        default=None,
        metadata={
            "name": "import",
            "type": "Element",
            "namespace": "",
        }
    )
    external_user_enabled: bool = field(
        default=False,
        metadata={
            "name": "externalUserEnabled",
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    minimum_site_role: Optional[SiteRoleType] = field(
        default=None,
        metadata={
            "name": "minimumSiteRole",
            "type": "Attribute",
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    user_count: Optional[int] = field(
        default=None,
        metadata={
            "name": "userCount",
            "type": "Attribute",
        }
    )
@dataclass
class LabelType:
    class Meta:
        name = "labelType"

    owner: Optional[UserType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    site: Optional[SiteType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    active: Optional[bool] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    category: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    content_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "contentId",
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    content_type: Optional[str] = field(
        default=None,
        metadata={
            "name": "contentType",
            "type": "Attribute",
        }
    )
    created_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "createdAt",
            "type": "Attribute",
        }
    )
    elevated: Optional[bool] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    message: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    updated_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "updatedAt",
            "type": "Attribute",
        }
    )
    user_display_name: Optional[str] = field(
        default=None,
        metadata={
            "name": "userDisplayName",
            "type": "Attribute",
        }
    )
    value: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
@dataclass
class LabelValueType:
    class Meta:
        name = "labelValueType"

    site: Optional[SiteType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    built_in: Optional[bool] = field(
        default=None,
        metadata={
            "name": "builtIn",
            "type": "Attribute",
        }
    )
    category: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    description: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    elevated_default: Optional[bool] = field(
        default=None,
        metadata={
            "name": "elevatedDefault",
            "type": "Attribute",
        }
    )
    internal: Optional[bool] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
@dataclass
class LensType:
    class Meta:
        name = "lensType"

    owner: Optional[UserType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
@dataclass
class LinkUserPayloadType:
    class Meta:
        name = "linkUserPayloadType"

    site_role: Optional[LinkUserSiteRoleType] = field(
        default=None,
        metadata={
            "name": "siteRole",
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    user: Optional[LinkUserType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    source_tenant_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "sourceTenantId",
            "type": "Attribute",
        }
    )
@dataclass
class MobileSecuritySettingsPolicyType:
    class Meta:
        name = "mobileSecuritySettingsPolicyType"

    android_config: Optional[PolicyType] = field(
        default=None,
        metadata={
            "name": "androidConfig",
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    ios_config: Optional[PolicyType] = field(
        default=None,
        metadata={
            "name": "iosConfig",
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    enabled: Optional[bool] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "required": True,
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "required": True,
        }
    )
@dataclass
class NotificationsPreferenceUpdateStatusListType:
    class Meta:
        name = "notificationsPreferenceUpdateStatusListType"

    notification_update_status: list[NotificationPreferenceUpdateStatusType] = field(
        default_factory=list,
        metadata={
            "name": "notificationUpdateStatus",
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class ProjectType:
    class Meta:
        name = "projectType"

    contents_counts: Optional[ContentsCountsType] = field(
        default=None,
        metadata={
            "name": "contentsCounts",
            "type": "Element",
            "namespace": "",
        }
    )
    owner: Optional[UserType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    content_permissions: Optional[ProjectTypeContentPermissions] = field(
        default=None,
        metadata={
            "name": "contentPermissions",
            "type": "Attribute",
        }
    )
    controlling_permissions_project_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "controllingPermissionsProjectId",
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    created_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "createdAt",
            "type": "Attribute",
        }
    )
    description: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    favorites_total: Optional[int] = field(
        default=None,
        metadata={
            "name": "favoritesTotal",
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    parent_project_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "parentProjectId",
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    top_level_project: Optional[bool] = field(
        default=None,
        metadata={
            "name": "topLevelProject",
            "type": "Attribute",
        }
    )
    updated_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "updatedAt",
            "type": "Attribute",
        }
    )
    writeable: Optional[bool] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
@dataclass
class RevisionType:
    class Meta:
        name = "revisionType"

    publisher: Optional[UserType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    current: Optional[bool] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    deleted: Optional[bool] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    published_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "publishedAt",
            "type": "Attribute",
        }
    )
    revision_number: Optional[int] = field(
        default=None,
        metadata={
            "name": "revisionNumber",
            "type": "Attribute",
        }
    )
    size_in_bytes: Optional[int] = field(
        default=None,
        metadata={
            "name": "sizeInBytes",
            "type": "Attribute",
        }
    )
@dataclass
class SessionType:
    class Meta:
        name = "sessionType"

    site: Optional[SiteType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    user: Optional[UserType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
@dataclass
class SiteListType:
    class Meta:
        name = "siteListType"

    site: list[SiteType] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class StatusNoteListType:
    class Meta:
        name = "statusNoteListType"

    status_note: list[StatusNoteType] = field(
        default_factory=list,
        metadata={
            "name": "statusNote",
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class TableType:
    class Meta:
        name = "tableType"

    certifier: Optional[UserType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    contact: Optional[UserType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    location: Optional[LocationType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    site: Optional[SiteType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    tags: Optional[TagListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    certification_note: Optional[str] = field(
        default=None,
        metadata={
            "name": "certificationNote",
            "type": "Attribute",
        }
    )
    description: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    is_certified: Optional[bool] = field(
        default=None,
        metadata={
            "name": "isCertified",
            "type": "Attribute",
        }
    )
    is_embedded: Optional[bool] = field(
        default=None,
        metadata={
            "name": "isEmbedded",
            "type": "Attribute",
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    schema: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
@dataclass
class TableauCredentialsType:
    class Meta:
        name = "tableauCredentialsType"

    site: Optional[SiteType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    user: Optional[UserType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    estimated_time_to_expiration: Optional[str] = field(
        default=None,
        metadata={
            "name": "estimatedTimeToExpiration",
            "type": "Attribute",
        }
    )
    jwt: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    password: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    personal_access_token_name: Optional[str] = field(
        default=None,
        metadata={
            "name": "personalAccessTokenName",
            "type": "Attribute",
        }
    )
    personal_access_token_secret: Optional[str] = field(
        default=None,
        metadata={
            "name": "personalAccessTokenSecret",
            "type": "Attribute",
        }
    )
    token: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
@dataclass
class TagBatchType:
    class Meta:
        name = "tagBatchType"

    contents: Optional[ContentListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    tags: Optional[TagListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
@dataclass
class UserListType:
    class Meta:
        name = "userListType"

    user: list[UserType] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class WebhookDestinationType:
    class Meta:
        name = "webhookDestinationType"

    webhook_destination_http: Optional[WebhookDestinationHttpType] = field(
        default=None,
        metadata={
            "name": "webhook-destination-http",
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
@dataclass
class CustomViewAsUserDefaultViewResultListType:
    class Meta:
        name = "customViewAsUserDefaultViewResultListType"

    custom_view_as_user_default_view_result: list[CustomViewAsUserDefaultViewResultType] = field(
        default_factory=list,
        metadata={
            "name": "customViewAsUserDefaultViewResult",
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class DataFreshnessPolicyType:
    class Meta:
        name = "dataFreshnessPolicyType"

    fresh_at_schedule: Optional[FreshAtScheduleType] = field(
        default=None,
        metadata={
            "name": "freshAtSchedule",
            "type": "Element",
            "namespace": "",
        }
    )
    fresh_every_schedule: Optional[FreshEveryScheduleType] = field(
        default=None,
        metadata={
            "name": "freshEverySchedule",
            "type": "Element",
            "namespace": "",
        }
    )
    option: Optional[DataFreshnessPolicyTypeOption] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
@dataclass
class DataQualityIndicatorListType:
    class Meta:
        name = "dataQualityIndicatorListType"

    data_quality_indicator: list[DataQualityIndicatorType] = field(
        default_factory=list,
        metadata={
            "name": "dataQualityIndicator",
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class DataQualityWarningListType:
    class Meta:
        name = "dataQualityWarningListType"

    data_quality_warning: list[DataQualityWarningType] = field(
        default_factory=list,
        metadata={
            "name": "dataQualityWarning",
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class DataSourceType:
    class Meta:
        name = "dataSourceType"

    connection_credentials: Optional[ConnectionCredentialsType] = field(
        default=None,
        metadata={
            "name": "connectionCredentials",
            "type": "Element",
            "namespace": "",
        }
    )
    location: Optional[LocationType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    owner: Optional[UserType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    project: Optional[ProjectType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    site: Optional[SiteType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    tags: Optional[TagListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    certification_note: Optional[str] = field(
        default=None,
        metadata={
            "name": "certificationNote",
            "type": "Attribute",
        }
    )
    connected_workbooks_count: Optional[int] = field(
        default=None,
        metadata={
            "name": "connectedWorkbooksCount",
            "type": "Attribute",
        }
    )
    content_url: Optional[str] = field(
        default=None,
        metadata={
            "name": "contentUrl",
            "type": "Attribute",
        }
    )
    created_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "createdAt",
            "type": "Attribute",
        }
    )
    database_name: Optional[str] = field(
        default=None,
        metadata={
            "name": "databaseName",
            "type": "Attribute",
        }
    )
    description: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    encrypt_extracts: Optional[str] = field(
        default=None,
        metadata={
            "name": "encryptExtracts",
            "type": "Attribute",
        }
    )
    favorites_total: Optional[int] = field(
        default=None,
        metadata={
            "name": "favoritesTotal",
            "type": "Attribute",
        }
    )
    has_alert: Optional[bool] = field(
        default=None,
        metadata={
            "name": "hasAlert",
            "type": "Attribute",
        }
    )
    has_extracts: Optional[bool] = field(
        default=None,
        metadata={
            "name": "hasExtracts",
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    is_certified: Optional[bool] = field(
        default=None,
        metadata={
            "name": "isCertified",
            "type": "Attribute",
        }
    )
    is_published: Optional[bool] = field(
        default=None,
        metadata={
            "name": "isPublished",
            "type": "Attribute",
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    parent_type: Optional[DataSourceTypeParentType] = field(
        default=None,
        metadata={
            "name": "parentType",
            "type": "Attribute",
        }
    )
    server_name: Optional[str] = field(
        default=None,
        metadata={
            "name": "serverName",
            "type": "Attribute",
        }
    )
    size: Optional[int] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    type_value: Optional[str] = field(
        default=None,
        metadata={
            "name": "type",
            "type": "Attribute",
        }
    )
    updated_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "updatedAt",
            "type": "Attribute",
        }
    )
    use_remote_query_agent: Optional[bool] = field(
        default=None,
        metadata={
            "name": "useRemoteQueryAgent",
            "type": "Attribute",
        }
    )
    webpage_url: Optional[str] = field(
        default=None,
        metadata={
            "name": "webpageUrl",
            "type": "Attribute",
        }
    )
@dataclass
class DataUpdateActionType:
    class Meta:
        name = "dataUpdateActionType"

    action: Optional[str] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    condition: Optional[DataUpdateConditionType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    source_file: Optional[str] = field(
        default=None,
        metadata={
            "name": "source-file",
            "type": "Element",
            "namespace": "",
            "min_length": 1,
            "max_length": 255,
        }
    )
    source_schema: Optional[str] = field(
        default=None,
        metadata={
            "name": "source-schema",
            "type": "Element",
            "namespace": "",
        }
    )
    source_table: Optional[str] = field(
        default=None,
        metadata={
            "name": "source-table",
            "type": "Element",
            "namespace": "",
        }
    )
    target_schema: Optional[str] = field(
        default=None,
        metadata={
            "name": "target-schema",
            "type": "Element",
            "namespace": "",
        }
    )
    target_table: Optional[str] = field(
        default=None,
        metadata={
            "name": "target-table",
            "type": "Element",
            "namespace": "",
        }
    )
    actions: Optional["DataUpdateActionType"] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class DatabaseAnchorResponseListType:
    class Meta:
        name = "databaseAnchorResponseListType"

    database_anchor: list[DatabaseAnchorResponseType] = field(
        default_factory=list,
        metadata={
            "name": "databaseAnchor",
            "type": "Element",
            "namespace": "",
            "min_occurs": 1,
        }
    )
@dataclass
class ExplanationType:
    class Meta:
        name = "explanationType"

    users: Optional[UserListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    type_value: Optional[ExplanationTypeType] = field(
        default=None,
        metadata={
            "name": "type",
            "type": "Attribute",
        }
    )
@dataclass
class FlowRunListType:
    class Meta:
        name = "flowRunListType"

    flow_runs: list[FlowRunType] = field(
        default_factory=list,
        metadata={
            "name": "flowRuns",
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class FlowType:
    class Meta:
        name = "flowType"

    owner: Optional[UserType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    parameters: Optional[FlowParameterListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    project: Optional[ProjectType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    site: Optional[SiteType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    tags: Optional[TagListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    created_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "createdAt",
            "type": "Attribute",
        }
    )
    description: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    file_type: Optional[str] = field(
        default=None,
        metadata={
            "name": "fileType",
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    updated_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "updatedAt",
            "type": "Attribute",
        }
    )
    webpage_url: Optional[str] = field(
        default=None,
        metadata={
            "name": "webpageUrl",
            "type": "Attribute",
        }
    )
@dataclass
class GroupListType:
    class Meta:
        name = "groupListType"

    group: list[GroupType] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class GroupSetType:
    class Meta:
        name = "groupSetType"

    group: list[GroupType] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    group_count: Optional[int] = field(
        default=None,
        metadata={
            "name": "groupCount",
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    local_id: Optional[int] = field(
        default=None,
        metadata={
            "name": "localId",
            "type": "Attribute",
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
@dataclass
class LabelListType:
    class Meta:
        name = "labelListType"

    label: list[LabelType] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class LabelValueListType:
    class Meta:
        name = "labelValueListType"

    label_value: list[LabelValueType] = field(
        default_factory=list,
        metadata={
            "name": "labelValue",
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class MobileSecuritySettingsListType:
    class Meta:
        name = "mobileSecuritySettingsListType"

    mobile_security_settings: list[MobileSecuritySettingsPolicyType] = field(
        default_factory=list,
        metadata={
            "name": "mobileSecuritySettings",
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class ProjectListType:
    class Meta:
        name = "projectListType"

    project: list[ProjectType] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class RevisionListType:
    class Meta:
        name = "revisionListType"

    revision: list[RevisionType] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class ScheduleType:
    class Meta:
        name = "scheduleType"

    frequency_details: Optional[FrequencyDetailsType] = field(
        default=None,
        metadata={
            "name": "frequencyDetails",
            "type": "Element",
            "namespace": "",
        }
    )
    created_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "createdAt",
            "type": "Attribute",
        }
    )
    end_schedule_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "endScheduleAt",
            "type": "Attribute",
        }
    )
    execution_order: Optional[ScheduleTypeExecutionOrder] = field(
        default=None,
        metadata={
            "name": "executionOrder",
            "type": "Attribute",
        }
    )
    frequency: Optional[ScheduleTypeFrequency] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    next_run_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "nextRunAt",
            "type": "Attribute",
        }
    )
    priority: Optional[int] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    state: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    type_value: Optional[ScheduleTypeType] = field(
        default=None,
        metadata={
            "name": "type",
            "type": "Attribute",
        }
    )
    updated_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "updatedAt",
            "type": "Attribute",
        }
    )
@dataclass
class SessionsType:
    class Meta:
        name = "sessionsType"

    session: list[SessionType] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class VirtualConnectionType:
    class Meta:
        name = "virtualConnectionType"

    content: Optional[str] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    owner: Optional[UserType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    project: Optional[ProjectType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    tags: Optional[TagListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    warnings: Optional[WarningListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    certification_note: Optional[str] = field(
        default=None,
        metadata={
            "name": "certificationNote",
            "type": "Attribute",
        }
    )
    created_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "createdAt",
            "type": "Attribute",
        }
    )
    has_extracts: Optional[bool] = field(
        default=None,
        metadata={
            "name": "hasExtracts",
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    is_certified: Optional[bool] = field(
        default=None,
        metadata={
            "name": "isCertified",
            "type": "Attribute",
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    started_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "startedAt",
            "type": "Attribute",
        }
    )
    updated_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "updatedAt",
            "type": "Attribute",
        }
    )
    webpage_url: Optional[str] = field(
        default=None,
        metadata={
            "name": "webpageUrl",
            "type": "Attribute",
        }
    )
@dataclass
class WebhookType:
    class Meta:
        name = "webhookType"

    owner: Optional[UserType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    webhook_destination: Optional[WebhookDestinationType] = field(
        default=None,
        metadata={
            "name": "webhook-destination",
            "type": "Element",
            "namespace": "",
        }
    )
    webhook_source: Optional[WebhookSourceType] = field(
        default=None,
        metadata={
            "name": "webhook-source",
            "type": "Element",
            "namespace": "",
        }
    )
    created_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "createdAt",
            "type": "Attribute",
        }
    )
    event: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    is_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "isEnabled",
            "type": "Attribute",
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    status_change_reason: Optional[str] = field(
        default=None,
        metadata={
            "name": "statusChangeReason",
            "type": "Attribute",
        }
    )
    updated_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "updatedAt",
            "type": "Attribute",
        }
    )
@dataclass
class ConnectionType:
    class Meta:
        name = "connectionType"

    connection_credentials: Optional[ConnectionCredentialsType] = field(
        default=None,
        metadata={
            "name": "connectionCredentials",
            "type": "Element",
            "namespace": "",
        }
    )
    datasource: Optional[DataSourceType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    db_class: Optional[str] = field(
        default=None,
        metadata={
            "name": "dbClass",
            "type": "Attribute",
        }
    )
    embed_password: Optional[bool] = field(
        default=None,
        metadata={
            "name": "embedPassword",
            "type": "Attribute",
        }
    )
    filename: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    google_sheet_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "googleSheetId",
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    password: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    query_tagging_enabled: Optional[bool] = field(
        default=None,
        metadata={
            "name": "queryTaggingEnabled",
            "type": "Attribute",
        }
    )
    scope: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    server_address: Optional[str] = field(
        default=None,
        metadata={
            "name": "serverAddress",
            "type": "Attribute",
        }
    )
    server_port: Optional[int] = field(
        default=None,
        metadata={
            "name": "serverPort",
            "type": "Attribute",
        }
    )
    type_value: Optional[str] = field(
        default=None,
        metadata={
            "name": "type",
            "type": "Attribute",
        }
    )
    user_name: Optional[str] = field(
        default=None,
        metadata={
            "name": "userName",
            "type": "Attribute",
        }
    )
@dataclass
class DataSourceListType:
    class Meta:
        name = "dataSourceListType"

    datasource: list[DataSourceType] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    include_all: Optional[bool] = field(
        default=None,
        metadata={
            "name": "includeAll",
            "type": "Attribute",
        }
    )
@dataclass
class FlowListType:
    class Meta:
        name = "flowListType"

    flow: list[FlowType] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class GranteeCapabilitiesType:
    class Meta:
        name = "granteeCapabilitiesType"

    group: Optional[GroupType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    group_set: Optional[GroupSetType] = field(
        default=None,
        metadata={
            "name": "groupSet",
            "type": "Element",
            "namespace": "",
        }
    )
    user: Optional[UserType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    capabilities: Optional["GranteeCapabilitiesType.Capabilities"] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )

    @dataclass
    class Capabilities:
        capability: list[CapabilityType] = field(
            default_factory=list,
            metadata={
                "type": "Element",
                "namespace": "",
                "min_occurs": 1,
            }
        )
@dataclass
class GroupSetListType:
    class Meta:
        name = "groupSetListType"

    group_set: list[GroupSetType] = field(
        default_factory=list,
        metadata={
            "name": "groupSet",
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class RunFlowJobType:
    class Meta:
        name = "runFlowJobType"

    flow: Optional[FlowType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    notes: Optional[str] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    flow_run_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "flowRunId",
            "type": "Attribute",
        }
    )
@dataclass
class ScheduleListType:
    class Meta:
        name = "scheduleListType"

    schedule: list[ScheduleType] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class SubscriptionType:
    class Meta:
        name = "subscriptionType"

    content: Optional[SubscriptionContentType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    schedule: Optional[ScheduleType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    user: Optional[UserType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    attach_image: Optional[bool] = field(
        default=None,
        metadata={
            "name": "attachImage",
            "type": "Attribute",
        }
    )
    attach_pdf: Optional[bool] = field(
        default=None,
        metadata={
            "name": "attachPdf",
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    message: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    page_orientation: Optional[str] = field(
        default=None,
        metadata={
            "name": "pageOrientation",
            "type": "Attribute",
        }
    )
    page_size_option: Optional[str] = field(
        default=None,
        metadata={
            "name": "pageSizeOption",
            "type": "Attribute",
        }
    )
    refresh_extract_triggered: Optional[bool] = field(
        default=None,
        metadata={
            "name": "refreshExtractTriggered",
            "type": "Attribute",
        }
    )
    subject: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    suspended: Optional[bool] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
@dataclass
class TaskRunFlowType:
    class Meta:
        name = "taskRunFlowType"

    flow: Optional[FlowType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    flow_run_spec: Optional[FlowRunSpecType] = field(
        default=None,
        metadata={
            "name": "flowRunSpec",
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    schedule: Optional[ScheduleType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    consecutive_failed_count: Optional[int] = field(
        default=None,
        metadata={
            "name": "consecutiveFailedCount",
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    priority: Optional[int] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    type_value: Optional[str] = field(
        default=None,
        metadata={
            "name": "type",
            "type": "Attribute",
        }
    )
@dataclass
class UpdateUploadedFileJobType:
    class Meta:
        name = "updateUploadedFileJobType"

    connection_luid: Optional[str] = field(
        default=None,
        metadata={
            "name": "connectionLuid",
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    datasource: Optional[DataSourceType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    notes: Optional[str] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
@dataclass
class VirtualConnectionListType:
    class Meta:
        name = "virtualConnectionListType"

    virtual_connection: list[VirtualConnectionType] = field(
        default_factory=list,
        metadata={
            "name": "virtualConnection",
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class WebhookListType:
    class Meta:
        name = "webhookListType"

    webhook: list[WebhookType] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class ConnectionListType:
    class Meta:
        name = "connectionListType"

    connection: list[ConnectionType] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class SubscriptionListType:
    class Meta:
        name = "subscriptionListType"

    subscription: list[SubscriptionType] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class WorkbookType:
    class Meta:
        name = "workbookType"

    connection_credentials: Optional[ConnectionCredentialsType] = field(
        default=None,
        metadata={
            "name": "connectionCredentials",
            "type": "Element",
            "namespace": "",
        }
    )
    connections: Optional[ConnectionListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    data_acceleration_config: Optional[DataAccelerationInfoType] = field(
        default=None,
        metadata={
            "name": "dataAccelerationConfig",
            "type": "Element",
            "namespace": "",
        }
    )
    data_freshness_policy: Optional[DataFreshnessPolicyType] = field(
        default=None,
        metadata={
            "name": "dataFreshnessPolicy",
            "type": "Element",
            "namespace": "",
        }
    )
    location: Optional[LocationType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    owner: Optional[UserType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    project: Optional[ProjectType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    site: Optional[SiteType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    tags: Optional[TagListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    views: Optional[ViewListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    content_url: Optional[str] = field(
        default=None,
        metadata={
            "name": "contentUrl",
            "type": "Attribute",
        }
    )
    created_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "createdAt",
            "type": "Attribute",
        }
    )
    default_view_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "defaultViewId",
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    description: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    encrypt_extracts: Optional[str] = field(
        default=None,
        metadata={
            "name": "encryptExtracts",
            "type": "Attribute",
        }
    )
    has_extracts: Optional[bool] = field(
        default=None,
        metadata={
            "name": "hasExtracts",
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    last_published_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "lastPublishedAt",
            "type": "Attribute",
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    primary_content_url: Optional[str] = field(
        default=None,
        metadata={
            "name": "primaryContentUrl",
            "type": "Attribute",
        }
    )
    recently_viewed: Optional[bool] = field(
        default=None,
        metadata={
            "name": "recentlyViewed",
            "type": "Attribute",
        }
    )
    share_description: Optional[str] = field(
        default=None,
        metadata={
            "name": "shareDescription",
            "type": "Attribute",
        }
    )
    sheet_count: Optional[int] = field(
        default=None,
        metadata={
            "name": "sheetCount",
            "type": "Attribute",
        }
    )
    show_tabs: Optional[str] = field(
        default=None,
        metadata={
            "name": "showTabs",
            "type": "Attribute",
        }
    )
    size: Optional[int] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    thumbnails_group_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "thumbnailsGroupId",
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    thumbnails_user_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "thumbnailsUserId",
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    updated_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "updatedAt",
            "type": "Attribute",
        }
    )
    webpage_url: Optional[str] = field(
        default=None,
        metadata={
            "name": "webpageUrl",
            "type": "Attribute",
        }
    )
@dataclass
class ExtractCreationJobType:
    class Meta:
        name = "extractCreationJobType"

    datasource: Optional[DataSourceType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    job_luid: Optional[str] = field(
        default=None,
        metadata={
            "name": "jobLuid",
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    notes: Optional[str] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    workbook: Optional[WorkbookType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
@dataclass
class ExtractType:
    class Meta:
        name = "extractType"

    datasource: Optional[DataSourceType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    workbook: Optional[WorkbookType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    priority: Optional[int] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    type_value: Optional[ExtractTypeType] = field(
        default=None,
        metadata={
            "name": "type",
            "type": "Attribute",
        }
    )
@dataclass
class TaskDataAccelerationType:
    class Meta:
        name = "taskDataAccelerationType"

    workbook: Optional[WorkbookType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    last_run_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "lastRunAt",
            "type": "Element",
            "namespace": "",
        }
    )
    schedule: Optional[ScheduleType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    consecutive_failed_count: Optional[int] = field(
        default=None,
        metadata={
            "name": "consecutiveFailedCount",
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    type_value: Optional[str] = field(
        default=None,
        metadata={
            "name": "type",
            "type": "Attribute",
        }
    )
@dataclass
class ViewType:
    class Meta:
        name = "viewType"

    data_acceleration_config: Optional[DataAccelerationInfoType] = field(
        default=None,
        metadata={
            "name": "dataAccelerationConfig",
            "type": "Element",
            "namespace": "",
        }
    )
    location: Optional[LocationType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    owner: Optional[UserType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    project: Optional[ProjectType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    tags: Optional[TagListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    usage: Optional["ViewType.Usage"] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    workbook: Optional[WorkbookType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    content_url: Optional[str] = field(
        default=None,
        metadata={
            "name": "contentUrl",
            "type": "Attribute",
        }
    )
    created_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "createdAt",
            "type": "Attribute",
        }
    )
    favorites_total: Optional[int] = field(
        default=None,
        metadata={
            "name": "favoritesTotal",
            "type": "Attribute",
        }
    )
    hidden: Optional[bool] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    recently_viewed: Optional[bool] = field(
        default=None,
        metadata={
            "name": "recentlyViewed",
            "type": "Attribute",
        }
    )
    sheet_type: Optional[str] = field(
        default=None,
        metadata={
            "name": "sheetType",
            "type": "Attribute",
        }
    )
    updated_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "updatedAt",
            "type": "Attribute",
        }
    )
    view_url_name: Optional[str] = field(
        default=None,
        metadata={
            "name": "viewUrlName",
            "type": "Attribute",
        }
    )

    @dataclass
    class Usage:
        total_view_count: Optional[int] = field(
            default=None,
            metadata={
                "name": "totalViewCount",
                "type": "Attribute",
                "required": True,
            }
        )
@dataclass
class WorkbookListType:
    class Meta:
        name = "workbookListType"

    workbook: list[WorkbookType] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class BroadcastViewType:
    class Meta:
        name = "broadcastViewType"

    view: Optional[ViewType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    allow_downloads: Optional[bool] = field(
        default=None,
        metadata={
            "name": "allowDownloads",
            "type": "Attribute",
        }
    )
    show_tabs: Optional[bool] = field(
        default=None,
        metadata={
            "name": "showTabs",
            "type": "Attribute",
        }
    )
    show_watermark: Optional[bool] = field(
        default=None,
        metadata={
            "name": "showWatermark",
            "type": "Attribute",
        }
    )
    url: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
@dataclass
class CustomViewType:
    class Meta:
        name = "customViewType"

    owner: Optional[UserType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    view: Optional[ViewType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    workbook: Optional[WorkbookType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    created_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "createdAt",
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    last_accessed_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "lastAccessedAt",
            "type": "Attribute",
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    shared: Optional[bool] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    updated_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "updatedAt",
            "type": "Attribute",
        }
    )
@dataclass
class DataAlertType:
    class Meta:
        name = "dataAlertType"

    owner: Optional[UserType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    recipients: Optional[DataAlertsRecipientListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    view: Optional[ViewType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    alert_condition: Optional[str] = field(
        default=None,
        metadata={
            "name": "alertCondition",
            "type": "Attribute",
        }
    )
    alert_threshold: Optional[str] = field(
        default=None,
        metadata={
            "name": "alertThreshold",
            "type": "Attribute",
        }
    )
    created_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "createdAt",
            "type": "Attribute",
        }
    )
    creator_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "creatorId",
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    frequency: Optional[DataAlertTypeFrequency] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    public: Optional[bool] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    subject: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    suspended: Optional[bool] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    updated_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "updatedAt",
            "type": "Attribute",
        }
    )
@dataclass
class ExtractListType:
    class Meta:
        name = "extractListType"

    extract: list[ExtractType] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class ExtractRefreshJobType:
    class Meta:
        name = "extractRefreshJobType"

    datasource: Optional[DataSourceType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    view: Optional[ViewType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    virtual_connection: Optional[VirtualConnectionType] = field(
        default=None,
        metadata={
            "name": "virtualConnection",
            "type": "Element",
            "namespace": "",
        }
    )
    workbook: Optional[WorkbookType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    notes: Optional[str] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
@dataclass
class MetricType:
    class Meta:
        name = "metricType"

    owner: Optional[UserType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    project: Optional[ProjectType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    site: Optional[SiteType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    tags: Optional[TagListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    underlying_view: Optional[ViewType] = field(
        default=None,
        metadata={
            "name": "underlyingView",
            "type": "Element",
            "namespace": "",
        }
    )
    created_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "createdAt",
            "type": "Attribute",
        }
    )
    description: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    name: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    suspended: Optional[bool] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    updated_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "updatedAt",
            "type": "Attribute",
        }
    )
    webpage_url: Optional[str] = field(
        default=None,
        metadata={
            "name": "webpageUrl",
            "type": "Attribute",
        }
    )
@dataclass
class RecentType:
    class Meta:
        name = "recentType"

    datasource: Optional[DataSourceType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    flow: Optional[FlowType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    project: Optional[ProjectType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    view: Optional[ViewType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    workbook: Optional[WorkbookType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class RecommendationDismissalType:
    class Meta:
        name = "recommendationDismissalType"

    view: Optional[ViewType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
@dataclass
class RecommendationType:
    class Meta:
        name = "recommendationType"

    view: Optional[ViewType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    explanation: Optional[ExplanationType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    content_type: Optional[RecommendedContentType] = field(
        default=None,
        metadata={
            "name": "contentType",
            "type": "Attribute",
        }
    )
    recommended_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "recommendedId",
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    score: Optional[Decimal] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
@dataclass
class TaskExtractRefreshType:
    class Meta:
        name = "taskExtractRefreshType"

    datasource: Optional[DataSourceType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    view: Optional[ViewType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    workbook: Optional[WorkbookType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    schedule: Optional[ScheduleType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    consecutive_failed_count: Optional[int] = field(
        default=None,
        metadata={
            "name": "consecutiveFailedCount",
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    incremental: Optional[bool] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    priority: Optional[int] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    type_value: Optional[str] = field(
        default=None,
        metadata={
            "name": "type",
            "type": "Attribute",
        }
    )
@dataclass
class BroadcastViewListType:
    class Meta:
        name = "broadcastViewListType"

    broadcast: list[BroadcastViewType] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class CustomViewListType:
    class Meta:
        name = "customViewListType"

    custom_view: list[CustomViewType] = field(
        default_factory=list,
        metadata={
            "name": "customView",
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class DataAlertListType:
    class Meta:
        name = "dataAlertListType"

    data_alert: list[DataAlertType] = field(
        default_factory=list,
        metadata={
            "name": "dataAlert",
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class FavoriteType:
    class Meta:
        name = "favoriteType"

    collection: Optional[CollectionType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    datasource: Optional[DataSourceType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    flow: Optional[FlowType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    metric: Optional[MetricType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    project: Optional[ProjectType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    view: Optional[ViewType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    workbook: Optional[WorkbookType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    added_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "addedAt",
            "type": "Attribute",
        }
    )
    label: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "required": True,
        }
    )
    parent_project_name: Optional[str] = field(
        default=None,
        metadata={
            "name": "parentProjectName",
            "type": "Attribute",
        }
    )
    position: Optional[int] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    target_owner_name: Optional[str] = field(
        default=None,
        metadata={
            "name": "targetOwnerName",
            "type": "Attribute",
        }
    )
@dataclass
class JobType:
    class Meta:
        name = "jobType"

    extract_creation_job: Optional[ExtractCreationJobType] = field(
        default=None,
        metadata={
            "name": "extractCreationJob",
            "type": "Element",
            "namespace": "",
        }
    )
    extract_refresh_job: Optional[ExtractRefreshJobType] = field(
        default=None,
        metadata={
            "name": "extractRefreshJob",
            "type": "Element",
            "namespace": "",
        }
    )
    run_flow_job_type: Optional[RunFlowJobType] = field(
        default=None,
        metadata={
            "name": "runFlowJobType",
            "type": "Element",
            "namespace": "",
        }
    )
    status_notes: Optional[StatusNoteListType] = field(
        default=None,
        metadata={
            "name": "statusNotes",
            "type": "Element",
            "namespace": "",
        }
    )
    subscription_job_type: Optional[SubscriptionJobType] = field(
        default=None,
        metadata={
            "name": "subscriptionJobType",
            "type": "Element",
            "namespace": "",
        }
    )
    update_uploaded_file_job: Optional[UpdateUploadedFileJobType] = field(
        default=None,
        metadata={
            "name": "updateUploadedFileJob",
            "type": "Element",
            "namespace": "",
        }
    )
    completed_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "completedAt",
            "type": "Attribute",
        }
    )
    created_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "createdAt",
            "type": "Attribute",
        }
    )
    finish_code: Optional[int] = field(
        default=None,
        metadata={
            "name": "finishCode",
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    mode: Optional[JobTypeMode] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    progress: Optional[int] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    started_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "startedAt",
            "type": "Attribute",
        }
    )
    type_value: Optional[JobTypeType] = field(
        default=None,
        metadata={
            "name": "type",
            "type": "Attribute",
        }
    )
    updated_at: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "updatedAt",
            "type": "Attribute",
        }
    )
@dataclass
class MetricListType:
    class Meta:
        name = "metricListType"

    metric: list[MetricType] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class PermissionsType:
    class Meta:
        name = "permissionsType"

    collection: Optional[CollectionType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    database: Optional[DatabaseType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    datasource: Optional[DataSourceType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    flow: Optional[FlowType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    lens: Optional[LensType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    metric: Optional[MetricType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    project: Optional[ProjectType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    table: Optional[TableType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    view: Optional[ViewType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    virtual_connection: Optional[VirtualConnectionType] = field(
        default=None,
        metadata={
            "name": "virtualConnection",
            "type": "Element",
            "namespace": "",
        }
    )
    workbook: Optional[WorkbookType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    grantee_capabilities: list[GranteeCapabilitiesType] = field(
        default_factory=list,
        metadata={
            "name": "granteeCapabilities",
            "type": "Element",
            "namespace": "",
        }
    )
    parent: Optional[ParentType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class RecentListType:
    class Meta:
        name = "recentListType"

    recent: list[RecentType] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class RecommendationListType:
    class Meta:
        name = "recommendationListType"

    recommendation: list[RecommendationType] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
            "max_occurs": 100,
        }
    )
@dataclass
class TaskType:
    class Meta:
        name = "taskType"

    data_acceleration: Optional[TaskDataAccelerationType] = field(
        default=None,
        metadata={
            "name": "dataAcceleration",
            "type": "Element",
            "namespace": "",
        }
    )
    extract_refresh: Optional[TaskExtractRefreshType] = field(
        default=None,
        metadata={
            "name": "extractRefresh",
            "type": "Element",
            "namespace": "",
        }
    )
    flow_run: Optional[TaskRunFlowType] = field(
        default=None,
        metadata={
            "name": "flowRun",
            "type": "Element",
            "namespace": "",
        }
    )
    schedule: Optional[ScheduleType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    consecutive_failed_count: Optional[int] = field(
        default=None,
        metadata={
            "name": "consecutiveFailedCount",
            "type": "Attribute",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    priority: Optional[int] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    run_now: Optional[bool] = field(
        default=None,
        metadata={
            "name": "runNow",
            "type": "Attribute",
        }
    )
    state: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
        }
    )
    type_value: Optional[str] = field(
        default=None,
        metadata={
            "name": "type",
            "type": "Attribute",
        }
    )
@dataclass
class FavoriteListType:
    class Meta:
        name = "favoriteListType"

    favorite: list[FavoriteType] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class LinkedTaskStepType:
    class Meta:
        name = "linkedTaskStepType"

    task: Optional[TaskType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    step_number: Optional[int] = field(
        default=None,
        metadata={
            "name": "stepNumber",
            "type": "Attribute",
        }
    )
    stop_downstream_tasks_on_failure: Optional[bool] = field(
        default=None,
        metadata={
            "name": "stopDownstreamTasksOnFailure",
            "type": "Attribute",
        }
    )
@dataclass
class TaskListType:
    class Meta:
        name = "taskListType"

    task: list[TaskType] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class TsRequest:
    class Meta:
        name = "tsRequest"
        namespace = "http://tableau.com/api"

    actions: list[DataUpdateActionType] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    associated_user_luid_mapping: Optional[AssociatedUserLuidMappingType] = field(
        default=None,
        metadata={
            "name": "associatedUserLuidMapping",
            "type": "Element",
            "namespace": "",
        }
    )
    broadcast_view_send: Optional[BroadcastViewSendType] = field(
        default=None,
        metadata={
            "name": "broadcastViewSend",
            "type": "Element",
            "namespace": "",
        }
    )
    connected_application: Optional[ConnectedApplicationType] = field(
        default=None,
        metadata={
            "name": "connectedApplication",
            "type": "Element",
            "namespace": "",
        }
    )
    connected_applications: Optional[ConnectedApplicationListType] = field(
        default=None,
        metadata={
            "name": "connectedApplications",
            "type": "Element",
            "namespace": "",
        }
    )
    connection: Optional[ConnectionType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    connections: Optional[ConnectionListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    content_list: Optional[ContentListType] = field(
        default=None,
        metadata={
            "name": "contentList",
            "type": "Element",
            "namespace": "",
        }
    )
    credentials: Optional[TableauCredentialsType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    custom_view: Optional[CustomViewType] = field(
        default=None,
        metadata={
            "name": "customView",
            "type": "Element",
            "namespace": "",
        }
    )
    data_alert: Optional[DataAlertType] = field(
        default=None,
        metadata={
            "name": "dataAlert",
            "type": "Element",
            "namespace": "",
        }
    )
    data_alert_create_alert: Optional[DataAlertCreateAlertType] = field(
        default=None,
        metadata={
            "name": "dataAlertCreateAlert",
            "type": "Element",
            "namespace": "",
        }
    )
    data_alerts: Optional[DataAlertListType] = field(
        default=None,
        metadata={
            "name": "dataAlerts",
            "type": "Element",
            "namespace": "",
        }
    )
    data_quality_indicator: Optional[DataQualityIndicatorType] = field(
        default=None,
        metadata={
            "name": "dataQualityIndicator",
            "type": "Element",
            "namespace": "",
        }
    )
    data_quality_trigger: Optional[DataQualityTriggerType] = field(
        default=None,
        metadata={
            "name": "dataQualityTrigger",
            "type": "Element",
            "namespace": "",
        }
    )
    data_quality_warning: Optional[DataQualityWarningType] = field(
        default=None,
        metadata={
            "name": "dataQualityWarning",
            "type": "Element",
            "namespace": "",
        }
    )
    database: Optional[DatabaseType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    database_anchor: Optional[DatabaseAnchorRequestType] = field(
        default=None,
        metadata={
            "name": "databaseAnchor",
            "type": "Element",
            "namespace": "",
        }
    )
    datasource: Optional[DataSourceType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    datasources: Optional[DataSourceListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    destination_server_url: Optional[str] = field(
        default=None,
        metadata={
            "name": "destinationServerUrl",
            "type": "Element",
            "namespace": "",
        }
    )
    destination_site_luid: Optional[str] = field(
        default=None,
        metadata={
            "name": "destinationSiteLuid",
            "type": "Element",
            "namespace": "",
        }
    )
    destination_site_url_namespace: Optional[str] = field(
        default=None,
        metadata={
            "name": "destinationSiteUrlNamespace",
            "type": "Element",
            "namespace": "",
        }
    )
    domain: Optional[DomainDirectiveType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    encrypted_keychain_list: Optional[EncryptedKeychainListType] = field(
        default=None,
        metadata={
            "name": "encryptedKeychainList",
            "type": "Element",
            "namespace": "",
        }
    )
    execution_context: Optional[LinkUserExecutionContextType] = field(
        default=None,
        metadata={
            "name": "executionContext",
            "type": "Element",
            "namespace": "",
        }
    )
    extensions_server_settings: Optional[ExtensionsServerSettingsType] = field(
        default=None,
        metadata={
            "name": "extensionsServerSettings",
            "type": "Element",
            "namespace": "",
        }
    )
    extensions_site_settings: Optional[ExtensionsSiteSettingsType] = field(
        default=None,
        metadata={
            "name": "extensionsSiteSettings",
            "type": "Element",
            "namespace": "",
        }
    )
    external_authorization_server: Optional[ExternalAuthorizationServerType] = field(
        default=None,
        metadata={
            "name": "externalAuthorizationServer",
            "type": "Element",
            "namespace": "",
        }
    )
    external_authorization_server_list: Optional[ExternalAuthorizationServerListType] = field(
        default=None,
        metadata={
            "name": "externalAuthorizationServerList",
            "type": "Element",
            "namespace": "",
        }
    )
    extract_refresh: Optional[TaskExtractRefreshType] = field(
        default=None,
        metadata={
            "name": "extractRefresh",
            "type": "Element",
            "namespace": "",
        }
    )
    favorite: Optional[FavoriteType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    favorite_orderings: Optional[FavoriteOrderingListType] = field(
        default=None,
        metadata={
            "name": "favoriteOrderings",
            "type": "Element",
            "namespace": "",
        }
    )
    flow: Optional[FlowType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    flow_run_spec: Optional[FlowRunSpecType] = field(
        default=None,
        metadata={
            "name": "flowRunSpec",
            "type": "Element",
            "namespace": "",
        }
    )
    group: Optional[GroupType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    group_set: Optional[GroupSetType] = field(
        default=None,
        metadata={
            "name": "groupSet",
            "type": "Element",
            "namespace": "",
        }
    )
    label: Optional[LabelType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    label_category: Optional[LabelCategoryType] = field(
        default=None,
        metadata={
            "name": "labelCategory",
            "type": "Element",
            "namespace": "",
        }
    )
    label_value: Optional[LabelValueType] = field(
        default=None,
        metadata={
            "name": "labelValue",
            "type": "Element",
            "namespace": "",
        }
    )
    link_site_migration: Optional[LinkSiteMigrationType] = field(
        default=None,
        metadata={
            "name": "linkSiteMigration",
            "type": "Element",
            "namespace": "",
        }
    )
    metric: Optional[MetricType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    mobile_security_settings_list: Optional[MobileSecuritySettingsListType] = field(
        default=None,
        metadata={
            "name": "mobileSecuritySettingsList",
            "type": "Element",
            "namespace": "",
        }
    )
    payload: Optional[LinkUserPayloadType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    permissions: Optional[PermissionsType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    personal_access_token: Optional[PersonalAccessTokenType] = field(
        default=None,
        metadata={
            "name": "personalAccessToken",
            "type": "Element",
            "namespace": "",
        }
    )
    project: Optional[ProjectType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    publish_to_salesforce: Optional[PublishToSalesforceRequestType] = field(
        default=None,
        metadata={
            "name": "publishToSalesforce",
            "type": "Element",
            "namespace": "",
        }
    )
    recommendation_dismissal: Optional[RecommendationDismissalType] = field(
        default=None,
        metadata={
            "name": "recommendationDismissal",
            "type": "Element",
            "namespace": "",
        }
    )
    schedule: Optional[ScheduleType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    service_token: Optional[ServiceTokenType] = field(
        default=None,
        metadata={
            "name": "serviceToken",
            "type": "Element",
            "namespace": "",
        }
    )
    service_tokens: Optional[ServiceTokenListType] = field(
        default=None,
        metadata={
            "name": "serviceTokens",
            "type": "Element",
            "namespace": "",
        }
    )
    site: Optional[SiteType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    site_oidcconfiguration: Optional[SiteOidcconfigurationType] = field(
        default=None,
        metadata={
            "name": "siteOIDCConfiguration",
            "type": "Element",
            "namespace": "",
        }
    )
    sites: Optional[SiteListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    subscription: Optional[SubscriptionType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    suggestion_feedback: Optional[SuggestionFeedbackType] = field(
        default=None,
        metadata={
            "name": "suggestionFeedback",
            "type": "Element",
            "namespace": "",
        }
    )
    table: Optional[TableType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    tag_batch: Optional[TagBatchType] = field(
        default=None,
        metadata={
            "name": "tagBatch",
            "type": "Element",
            "namespace": "",
        }
    )
    tags: Optional[TagListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    tas_site_oauth_client: Optional[TasSiteOauthClientType] = field(
        default=None,
        metadata={
            "name": "tasSiteOAuthClient",
            "type": "Element",
            "namespace": "",
        }
    )
    tas_site_oauth_clients: Optional[TasSiteOauthClientListType] = field(
        default=None,
        metadata={
            "name": "tasSiteOAuthClients",
            "type": "Element",
            "namespace": "",
        }
    )
    task: Optional[TaskType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    user: Optional[UserType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    user_notifications_preference: Optional[UserNotificationsPreferenceType] = field(
        default=None,
        metadata={
            "name": "userNotificationsPreference",
            "type": "Element",
            "namespace": "",
        }
    )
    user_notifications_preferences: Optional[UserNotificationsPreferenceListType] = field(
        default=None,
        metadata={
            "name": "userNotificationsPreferences",
            "type": "Element",
            "namespace": "",
        }
    )
    users: Optional[UserListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    view: Optional[ViewType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    virtual_connection: Optional[VirtualConnectionType] = field(
        default=None,
        metadata={
            "name": "virtualConnection",
            "type": "Element",
            "namespace": "",
        }
    )
    virtual_connection_connections: Optional[VirtualConnectionConnectionsType] = field(
        default=None,
        metadata={
            "name": "virtualConnectionConnections",
            "type": "Element",
            "namespace": "",
        }
    )
    virtual_connection_source_connection: Optional[VirtualConnectionSourceConnectionType] = field(
        default=None,
        metadata={
            "name": "virtualConnectionSourceConnection",
            "type": "Element",
            "namespace": "",
        }
    )
    virtual_connections: Optional[VirtualConnectionListType] = field(
        default=None,
        metadata={
            "name": "virtualConnections",
            "type": "Element",
            "namespace": "",
        }
    )
    webhook: Optional[WebhookType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    workbook: Optional[WorkbookType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class LinkedTaskStepListType:
    class Meta:
        name = "linkedTaskStepListType"

    linked_task_steps: list[LinkedTaskStepType] = field(
        default_factory=list,
        metadata={
            "name": "linkedTaskSteps",
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class LinkedTaskType:
    class Meta:
        name = "linkedTaskType"

    linked_task_steps: Optional[LinkedTaskStepListType] = field(
        default=None,
        metadata={
            "name": "linkedTaskSteps",
            "type": "Element",
            "namespace": "",
            "required": True,
        }
    )
    schedule: Optional[ScheduleType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "pattern": r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
        }
    )
    num_steps: Optional[int] = field(
        default=None,
        metadata={
            "name": "numSteps",
            "type": "Attribute",
        }
    )
@dataclass
class LinkedTaskListType:
    class Meta:
        name = "linkedTaskListType"

    linked_tasks: list[LinkedTaskType] = field(
        default_factory=list,
        metadata={
            "name": "linkedTasks",
            "type": "Element",
            "namespace": "",
        }
    )
@dataclass
class TsResponse:
    class Meta:
        name = "tsResponse"
        namespace = "http://tableau.com/api"

    pagination: Optional[PaginationType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    custom_views: Optional[CustomViewListType] = field(
        default=None,
        metadata={
            "name": "customViews",
            "type": "Element",
            "namespace": "",
        }
    )
    datasources: Optional[DataSourceListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    extracts: Optional[ExtractListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    flow_output_steps: Optional[FlowOutputStepListType] = field(
        default=None,
        metadata={
            "name": "flowOutputSteps",
            "type": "Element",
            "namespace": "",
        }
    )
    flow_runs: Optional[FlowRunListType] = field(
        default=None,
        metadata={
            "name": "flowRuns",
            "type": "Element",
            "namespace": "",
        }
    )
    flows: Optional[FlowListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    group_sets: Optional[GroupSetListType] = field(
        default=None,
        metadata={
            "name": "groupSets",
            "type": "Element",
            "namespace": "",
        }
    )
    groups: Optional[GroupListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    linked_tasks: Optional[LinkedTaskListType] = field(
        default=None,
        metadata={
            "name": "linkedTasks",
            "type": "Element",
            "namespace": "",
        }
    )
    metrics: Optional[MetricListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    projects: Optional[ProjectListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    revisions: Optional[RevisionListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    schedules: Optional[ScheduleListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    sites: Optional[SiteListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    subscriptions: Optional[SubscriptionListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    users: Optional[UserListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    workbooks: Optional[WorkbookListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    associated_user_luid_list: Optional[AssociatedUserLuidListType] = field(
        default=None,
        metadata={
            "name": "associatedUserLuidList",
            "type": "Element",
            "namespace": "",
        }
    )
    background_job: Optional[BackgroundJobType] = field(
        default=None,
        metadata={
            "name": "backgroundJob",
            "type": "Element",
            "namespace": "",
        }
    )
    background_jobs: Optional[BackgroundJobListType] = field(
        default=None,
        metadata={
            "name": "backgroundJobs",
            "type": "Element",
            "namespace": "",
        }
    )
    broadcast_view: Optional[BroadcastViewType] = field(
        default=None,
        metadata={
            "name": "broadcastView",
            "type": "Element",
            "namespace": "",
        }
    )
    broadcast_views: Optional[BroadcastViewListType] = field(
        default=None,
        metadata={
            "name": "broadcastViews",
            "type": "Element",
            "namespace": "",
        }
    )
    connected_application: Optional[ConnectedApplicationType] = field(
        default=None,
        metadata={
            "name": "connectedApplication",
            "type": "Element",
            "namespace": "",
        }
    )
    connected_application_secret: Optional[ConnectedApplicationSecretType] = field(
        default=None,
        metadata={
            "name": "connectedApplicationSecret",
            "type": "Element",
            "namespace": "",
        }
    )
    connected_applications: Optional[ConnectedApplicationListType] = field(
        default=None,
        metadata={
            "name": "connectedApplications",
            "type": "Element",
            "namespace": "",
        }
    )
    connection: Optional[ConnectionType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    connections: Optional[ConnectionListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    content_location: Optional[LocationType] = field(
        default=None,
        metadata={
            "name": "contentLocation",
            "type": "Element",
            "namespace": "",
        }
    )
    credentials: Optional[TableauCredentialsType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    custom_view: Optional[CustomViewType] = field(
        default=None,
        metadata={
            "name": "customView",
            "type": "Element",
            "namespace": "",
        }
    )
    custom_view_as_user_default_results: Optional[CustomViewAsUserDefaultViewResultListType] = field(
        default=None,
        metadata={
            "name": "customViewAsUserDefaultResults",
            "type": "Element",
            "namespace": "",
        }
    )
    data_alert: Optional[DataAlertType] = field(
        default=None,
        metadata={
            "name": "dataAlert",
            "type": "Element",
            "namespace": "",
        }
    )
    data_alert_create_alert: Optional[DataAlertCreateAlertType] = field(
        default=None,
        metadata={
            "name": "dataAlertCreateAlert",
            "type": "Element",
            "namespace": "",
        }
    )
    data_alert_update_results: Optional[DataAlertUpdateStatusListType] = field(
        default=None,
        metadata={
            "name": "dataAlertUpdateResults",
            "type": "Element",
            "namespace": "",
        }
    )
    data_alerts: Optional[DataAlertListType] = field(
        default=None,
        metadata={
            "name": "dataAlerts",
            "type": "Element",
            "namespace": "",
        }
    )
    data_alerts_recipient: Optional[DataAlertsRecipientType] = field(
        default=None,
        metadata={
            "name": "dataAlertsRecipient",
            "type": "Element",
            "namespace": "",
        }
    )
    data_alerts_recipient_list: Optional[DataAlertsRecipientListType] = field(
        default=None,
        metadata={
            "name": "dataAlertsRecipientList",
            "type": "Element",
            "namespace": "",
        }
    )
    data_quality_indicator: Optional[DataQualityIndicatorType] = field(
        default=None,
        metadata={
            "name": "dataQualityIndicator",
            "type": "Element",
            "namespace": "",
        }
    )
    data_quality_indicator_list: Optional[DataQualityIndicatorListType] = field(
        default=None,
        metadata={
            "name": "dataQualityIndicatorList",
            "type": "Element",
            "namespace": "",
        }
    )
    data_quality_trigger: Optional[DataQualityTriggerType] = field(
        default=None,
        metadata={
            "name": "dataQualityTrigger",
            "type": "Element",
            "namespace": "",
        }
    )
    data_quality_trigger_list: Optional[DataQualityTriggerListType] = field(
        default=None,
        metadata={
            "name": "dataQualityTriggerList",
            "type": "Element",
            "namespace": "",
        }
    )
    data_quality_warning: Optional[DataQualityWarningType] = field(
        default=None,
        metadata={
            "name": "dataQualityWarning",
            "type": "Element",
            "namespace": "",
        }
    )
    data_quality_warning_list: Optional[DataQualityWarningListType] = field(
        default=None,
        metadata={
            "name": "dataQualityWarningList",
            "type": "Element",
            "namespace": "",
        }
    )
    database: Optional[DatabaseType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    database_anchors: Optional[DatabaseAnchorResponseListType] = field(
        default=None,
        metadata={
            "name": "databaseAnchors",
            "type": "Element",
            "namespace": "",
        }
    )
    datasource: Optional[DataSourceType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    domain: Optional[DomainDirectiveType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    domain_list: Optional[DomainDirectiveListType] = field(
        default=None,
        metadata={
            "name": "domainList",
            "type": "Element",
            "namespace": "",
        }
    )
    downgrade_info: Optional[DegradationListType] = field(
        default=None,
        metadata={
            "name": "downgradeInfo",
            "type": "Element",
            "namespace": "",
        }
    )
    encrypted_keychain_list: Optional[EncryptedKeychainListType] = field(
        default=None,
        metadata={
            "name": "encryptedKeychainList",
            "type": "Element",
            "namespace": "",
        }
    )
    error: Optional[ErrorType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    extension_url_status: Optional[ExtensionUrlStatusType] = field(
        default=None,
        metadata={
            "name": "extensionUrlStatus",
            "type": "Element",
            "namespace": "",
        }
    )
    extensions_server_settings: Optional[ExtensionsServerSettingsType] = field(
        default=None,
        metadata={
            "name": "extensionsServerSettings",
            "type": "Element",
            "namespace": "",
        }
    )
    extensions_site_settings: Optional[ExtensionsSiteSettingsType] = field(
        default=None,
        metadata={
            "name": "extensionsSiteSettings",
            "type": "Element",
            "namespace": "",
        }
    )
    external_authorization_server: Optional[ExternalAuthorizationServerType] = field(
        default=None,
        metadata={
            "name": "externalAuthorizationServer",
            "type": "Element",
            "namespace": "",
        }
    )
    external_authorization_server_list: Optional[ExternalAuthorizationServerListType] = field(
        default=None,
        metadata={
            "name": "externalAuthorizationServerList",
            "type": "Element",
            "namespace": "",
        }
    )
    extract_refresh: Optional[TaskExtractRefreshType] = field(
        default=None,
        metadata={
            "name": "extractRefresh",
            "type": "Element",
            "namespace": "",
        }
    )
    favorites: Optional[FavoriteListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    file_upload: Optional[FileUploadType] = field(
        default=None,
        metadata={
            "name": "fileUpload",
            "type": "Element",
            "namespace": "",
        }
    )
    flow: Optional[FlowType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    flow_run: Optional[FlowRunType] = field(
        default=None,
        metadata={
            "name": "flowRun",
            "type": "Element",
            "namespace": "",
        }
    )
    flow_warnings: Optional[FlowWarningsListContainerType] = field(
        default=None,
        metadata={
            "name": "flowWarnings",
            "type": "Element",
            "namespace": "",
        }
    )
    generative_ai_check_registration: Optional[StatusType] = field(
        default=None,
        metadata={
            "name": "generativeAiCheckRegistration",
            "type": "Element",
            "namespace": "",
        }
    )
    generative_ai_registration: Optional[GenerativeAiRegistrationType] = field(
        default=None,
        metadata={
            "name": "generativeAiRegistration",
            "type": "Element",
            "namespace": "",
        }
    )
    group: Optional[GroupType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    group_set: Optional[GroupSetType] = field(
        default=None,
        metadata={
            "name": "groupSet",
            "type": "Element",
            "namespace": "",
        }
    )
    job: Optional[JobType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    label: Optional[LabelType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    label_category: Optional[LabelCategoryType] = field(
        default=None,
        metadata={
            "name": "labelCategory",
            "type": "Element",
            "namespace": "",
        }
    )
    label_category_list: Optional[LabelCategoryListType] = field(
        default=None,
        metadata={
            "name": "labelCategoryList",
            "type": "Element",
            "namespace": "",
        }
    )
    label_list: Optional[LabelListType] = field(
        default=None,
        metadata={
            "name": "labelList",
            "type": "Element",
            "namespace": "",
        }
    )
    label_value: Optional[LabelValueType] = field(
        default=None,
        metadata={
            "name": "labelValue",
            "type": "Element",
            "namespace": "",
        }
    )
    label_value_list: Optional[LabelValueListType] = field(
        default=None,
        metadata={
            "name": "labelValueList",
            "type": "Element",
            "namespace": "",
        }
    )
    linked_task: Optional[LinkedTaskType] = field(
        default=None,
        metadata={
            "name": "linkedTask",
            "type": "Element",
            "namespace": "",
        }
    )
    linked_task_job: Optional[LinkedTaskJobType] = field(
        default=None,
        metadata={
            "name": "linkedTaskJob",
            "type": "Element",
            "namespace": "",
        }
    )
    metric: Optional[MetricType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    mobile_security_settings_list: Optional[MobileSecuritySettingsListType] = field(
        default=None,
        metadata={
            "name": "mobileSecuritySettingsList",
            "type": "Element",
            "namespace": "",
        }
    )
    notification_preference_update_status: Optional[NotificationPreferenceUpdateStatusType] = field(
        default=None,
        metadata={
            "name": "notificationPreferenceUpdateStatus",
            "type": "Element",
            "namespace": "",
        }
    )
    notification_update_result: Optional[NotificationsPreferenceUpdateStatusListType] = field(
        default=None,
        metadata={
            "name": "notificationUpdateResult",
            "type": "Element",
            "namespace": "",
        }
    )
    permissions: Optional[PermissionsType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    personal_access_tokens: Optional[PersonalAccessTokenListType] = field(
        default=None,
        metadata={
            "name": "personalAccessTokens",
            "type": "Element",
            "namespace": "",
        }
    )
    personal_space: Optional[PersonalSpaceType] = field(
        default=None,
        metadata={
            "name": "personalSpace",
            "type": "Element",
            "namespace": "",
        }
    )
    project: Optional[ProjectType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    publish_to_salesforce: Optional[PublishToSalesforceBatchType] = field(
        default=None,
        metadata={
            "name": "publishToSalesforce",
            "type": "Element",
            "namespace": "",
        }
    )
    recents: Optional[RecentListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    recommendations: Optional[RecommendationListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    salesforce_apps: Optional[SalesforceAppListType] = field(
        default=None,
        metadata={
            "name": "salesforceApps",
            "type": "Element",
            "namespace": "",
        }
    )
    schedule: Optional[ScheduleType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    server_info: Optional[ServerInfo] = field(
        default=None,
        metadata={
            "name": "serverInfo",
            "type": "Element",
            "namespace": "",
        }
    )
    server_settings: Optional[ServerSettings] = field(
        default=None,
        metadata={
            "name": "serverSettings",
            "type": "Element",
            "namespace": "",
        }
    )
    service_token: Optional[ServiceTokenType] = field(
        default=None,
        metadata={
            "name": "serviceToken",
            "type": "Element",
            "namespace": "",
        }
    )
    service_tokens: Optional[ServiceTokenListType] = field(
        default=None,
        metadata={
            "name": "serviceTokens",
            "type": "Element",
            "namespace": "",
        }
    )
    session: Optional[SessionType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    sessions: Optional[SessionsType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    site: Optional[SiteType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    site_encryption_metadata: Optional[SiteEncryptionMetadata] = field(
        default=None,
        metadata={
            "name": "siteEncryptionMetadata",
            "type": "Element",
            "namespace": "",
        }
    )
    site_oidcconfiguration: Optional[SiteOidcconfigurationType] = field(
        default=None,
        metadata={
            "name": "siteOIDCConfiguration",
            "type": "Element",
            "namespace": "",
        }
    )
    site_oidcconfigurations: Optional[SiteOidcconfigurationListType] = field(
        default=None,
        metadata={
            "name": "siteOIDCConfigurations",
            "type": "Element",
            "namespace": "",
        }
    )
    status: Optional[str] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    subscription: Optional[SubscriptionType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    suggestion: Optional[SuggestionType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    suggestion_list: Optional[SuggestionListType] = field(
        default=None,
        metadata={
            "name": "suggestionList",
            "type": "Element",
            "namespace": "",
        }
    )
    table: Optional[TableType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    tag_batch: Optional[TagBatchType] = field(
        default=None,
        metadata={
            "name": "tagBatch",
            "type": "Element",
            "namespace": "",
        }
    )
    tags: Optional[TagListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    tas_site_oauth_client: Optional[TasSiteOauthClientType] = field(
        default=None,
        metadata={
            "name": "tasSiteOAuthClient",
            "type": "Element",
            "namespace": "",
        }
    )
    tas_site_oauth_clients: Optional[TasSiteOauthClientListType] = field(
        default=None,
        metadata={
            "name": "tasSiteOAuthClients",
            "type": "Element",
            "namespace": "",
        }
    )
    task: Optional[TaskType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    tasks: Optional[TaskListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    uri: Optional[str] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    user: Optional[UserType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    user_notifications_preference: Optional[UserNotificationsPreferenceType] = field(
        default=None,
        metadata={
            "name": "userNotificationsPreference",
            "type": "Element",
            "namespace": "",
        }
    )
    user_notifications_preferences: Optional[UserNotificationsPreferenceListType] = field(
        default=None,
        metadata={
            "name": "userNotificationsPreferences",
            "type": "Element",
            "namespace": "",
        }
    )
    user_operation_result: Optional[LinkUserOperationResultType] = field(
        default=None,
        metadata={
            "name": "userOperationResult",
            "type": "Element",
            "namespace": "",
        }
    )
    view: Optional[ViewType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    views: Optional[ViewListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    virtual_connection: Optional[VirtualConnectionType] = field(
        default=None,
        metadata={
            "name": "virtualConnection",
            "type": "Element",
            "namespace": "",
        }
    )
    virtual_connection_connections: Optional[VirtualConnectionConnectionsType] = field(
        default=None,
        metadata={
            "name": "virtualConnectionConnections",
            "type": "Element",
            "namespace": "",
        }
    )
    virtual_connection_source_connection: Optional[VirtualConnectionSourceConnectionType] = field(
        default=None,
        metadata={
            "name": "virtualConnectionSourceConnection",
            "type": "Element",
            "namespace": "",
        }
    )
    virtual_connections: Optional[VirtualConnectionListType] = field(
        default=None,
        metadata={
            "name": "virtualConnections",
            "type": "Element",
            "namespace": "",
        }
    )
    webhook: Optional[WebhookType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    webhook_test_result: Optional[WebhookTestResultType] = field(
        default=None,
        metadata={
            "name": "webhookTestResult",
            "type": "Element",
            "namespace": "",
        }
    )
    webhooks: Optional[WebhookListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    workbook: Optional[WorkbookType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
    warnings: Optional[WarningListType] = field(
        default=None,
        metadata={
            "type": "Element",
            "namespace": "",
        }
    )
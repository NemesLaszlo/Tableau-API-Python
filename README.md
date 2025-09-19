# Tableau API Client

A comprehensive Python client library for the Tableau Server REST API, providing easy-to-use interfaces for managing Tableau Server resources programmatically.

## Features

- **Complete API Coverage**: Support for all major Tableau Server REST API endpoints
- **Type Safety**: Full type hints and auto-generated models from Tableau's API specification
- **Authentication Management**: Username/password and Personal Access Token (PAT) authentication
- **Version Compatibility**: Support for multiple Tableau API versions (2.0 - 3.23+)
- **Pagination Support**: Built-in pagination handling for large result sets
- **Error Handling**: Comprehensive exception handling with detailed error messages
- **Content Management**: Query, filter, and manage workbooks, data sources, projects, users, and more
- **Tagging System**: Add, remove, and manage tags on content
- **Modular Design**: Organized client modules for different API domains

## Installation

```bash
pip install tableau_api_client
```

## Quick Start

### Basic Authentication and Querying

```python
from tableau_api_client import TableauApiClient

# Initialize the client
client = TableauApiClient(
    tableau_base_uri='https://your-tableau-server.com',
    api_version='3.19'
)

# Sign in with username/password
session = client.authentication.sign_in(
    user_name='your_username',
    password='your_password',
    site_content_url='your_site'  # Use '' for default site with basic configuration
)

# List all projects with pagination
all_projects = []
page_number = 1

while True:
    pagination, projects = client.projects.query_projects(
        session=session,
        page_size=100,
        page_number=page_number
    )
    
    if not projects:
        break
        
    all_projects.extend(projects)
    
    # Check if we've reached the end
    if len(projects) < 100:
        break
        
    page_number += 1

for project in all_projects:
    print(f"Project: {project.name} (ID: {project.id})")

# Sign out
client.authentication.sign_out(session)
```

### Personal Access Token Authentication

```python
# Sign in with PAT (recommended for automation)
session = client.authentication.sign_in_with_pat(
    token_name='your_pat_name',
    token='your_pat_token',
    site_content_url='your_site'
)
```

## API Modules

The client is organized into logical modules:

- **authentication**: Sign in/out, manage sessions and tokens
- **projects**: Project operations and management
- **workbooks_views**: Workbook and view management
- **datasources**: Data source management
- **users_groups**: User and group management
- **sites**: Site administration
- **permissions**: Permission management
- **publishing**: Publish workbooks and data sources
- **jobs_tasks_schedules**: Background jobs and schedules
- **server**: Server information and settings
- **subscriptions**: Manage subscriptions
- **favorites**: Manage favorites
- **flows**: Tableau flows
- **revisions**: Content revisions

## Working with Content

### Querying Workbooks

```python
# Get all workbooks with pagination
all_workbooks = []
page_number = 1

while True:
    pagination, workbooks = client.workbooks_views.query_workbooks_for_site(
        session=session,
        page_size=50,
        page_number=page_number
    )
    
    if not workbooks:
        break
        
    all_workbooks.extend(workbooks)
    
    if len(workbooks) < 50:  # Last page
        break
        
    page_number += 1

# Display workbook information
for workbook in all_workbooks:
    print(f"Workbook: {workbook.name}")
    print(f"  Project: {workbook.project.name}")
    print(f"  Owner: {workbook.owner.name}")
    print(f"  Created: {workbook.created_at}")
    print(f"  Size: {workbook.size}")
```

### Querying Data Sources

```python
# Get all data sources with pagination
pagination, datasources = client.datasources.query_data_sources(
    session=session,
    page_size=100,
    sort_expression="name:asc"  # Sort by name
)

for datasource in datasources:
    print(f"Data Source: {datasource.name}")
    print(f"  Type: {datasource.type_value}")
    print(f"  Project: {datasource.project.name}")
    print(f"  Certified: {getattr(datasource, 'is_certified', False)}")
```

### Getting Detailed Information

```python
# Get detailed workbook information
detailed_workbook = client.workbooks_views.query_workbook(
    session=session,
    workbook_id=workbook_id
)

print(f"Workbook: {detailed_workbook.name}")
print(f"Description: {detailed_workbook.description}")
print(f"Show Tabs: {detailed_workbook.show_tabs}")

# List views in the workbook
if detailed_workbook.views and detailed_workbook.views.view:
    print("Views:")
    for view in detailed_workbook.views.view:
        print(f"  - {view.name} (ID: {view.id})")
```

### Content Tagging

```python
# Add tags to a workbook
test_tags = ["production", "financial-data", "automated-refresh"]

added_tags = client.workbooks_views.add_tags_to_workbook(
    session=session,
    workbook_id=workbook_id,
    tags_to_add=test_tags
)

print(f"Added {len(added_tags)} tags:")
for tag in added_tags:
    print(f"  - {tag.label}")

# Add tags to a data source
datasource_tags = ["raw-data", "daily-refresh"]

added_ds_tags = client.datasources.add_tags_to_data_source(
    session=session,
    datasource_id=datasource_id,
    tags_to_add=datasource_tags
)

# Remove tags
client.workbooks_views.delete_tag_from_workbook(
    session=session,
    workbook_id=workbook_id,
    tag_name="production"
)
```

### Filtering and Sorting

```python
# Filter workbooks by name
pagination, filtered_workbooks = client.workbooks_views.query_workbooks_for_site(
    session=session,
    filter_expression="name:eq:Sales Dashboard",
    page_size=10
)

# Sort data sources by creation date
pagination, sorted_datasources = client.datasources.query_data_sources(
    session=session,
    sort_expression="createdAt:desc",
    page_size=20
)
```

## Advanced Usage

### Custom Configuration

```python
from tableau_api_client import TableauApiClient
import logging
from datetime import timedelta

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('tableau_client')

# Initialize with custom settings
client = TableauApiClient(
    tableau_base_uri='https://your-server.com',
    api_version='3.19',
    logger=logger,
    timeout=timedelta(minutes=10),
    ignore_ssl_errors=False  # Set to True for self-signed certificates
)
```

### Error Handling

```python
from tableau_api_client.exceptions import (
    TableauRequestException,
    TableauApiVersionException,
    TableauOnlineNotSupportedException
)

try:
    session = client.authentication.sign_in(username, password, site_name)
    
    # Perform operations
    pagination, projects = client.projects.query_projects(session)
    
except TableauRequestException as e:
    print(f"Request failed: {e.message}")
    print(f"Status code: {e.status_code}")
    if e.has_details:
        print(f"Error details: {e.details}")
        
except TableauApiVersionException as e:
    print(f"API version too low: {e.message}")
    print(f"Required: {e.version_required}, Current: {e.current_version}")
    
except TableauOnlineNotSupportedException as e:
    print(f"Feature not available in Tableau Online: {e.message}")
```

### Publishing Content

```python
# Publishing a workbook
with open('my_workbook.twbx', 'rb') as workbook_file:
    published_workbook = client.publishing.publish_workbook(
        session=session,
        project_id='project-id',
        workbook_name='My Workbook',
        file_upload=workbook_file,
        overwrite=True
    )

# Publishing a data source
with open('my_datasource.tdsx', 'rb') as datasource_file:
    published_ds = client.publishing.publish_datasource(
        session=session,
        project_id='project-id',
        datasource_name='My Data Source',
        file_upload=datasource_file
    )
```

## API Version Compatibility

The client automatically handles version compatibility and will raise `TableauApiVersionException` if you try to use a feature that requires a newer API version than configured.

```python
try:
    # This method might require API version 3.0+
    result = client.some_advanced_feature(session)
except TableauApiVersionException as e:
    print(f"Upgrade to API version {e.version_required} to use this feature")
    print(f"Current version: {e.current_version}")
```

## Supported Tableau Versions

- Tableau Server 2018.1 and later
- Tableau Online (Cloud)
- API versions 2.0 through 3.23+

## Requirements

- Python 3.7+
- requests >= 2.28.0
- xsdata[lxml] >= 23.8
- typing-extensions >= 4.0.0
- urllib3 >= 2.5.0

## License

This project is licensed under the MIT License - see the [LICENSE](https://github.com/NemesLaszlo/Tableau-API-Python/blob/main/LICENSE) file for details.

## Support

- [GitHub Issues](https://github.com/NemesLaszlo/Tableau-API-Python/issues)
- [Tableau REST API Documentation](https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api.htm)

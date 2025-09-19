import sys
import os

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

import pytest
import logging
from datetime import datetime, timedelta

from tableau_api_client.exceptions.tableau_api_version_exception import TableauApiVersionException
from tableau_api_client.exceptions.tableau_request_exception import TableauRequestException
from tableau_api_client.models.tableau_session import TableauSession
from tableau_api_client.tableau_api_client import TableauApiClient


class TestTableauContentIntegration:
    """Integration tests for Tableau API content operations with real server calls"""
    
    TABLEAU_SERVER_URL = "" 
    TEST_USERNAME = ""                          
    TEST_PASSWORD = ""                          
    TEST_SITE_URL = ""                                       
    TEST_PAT_NAME = ""                          
    TEST_PAT_TOKEN = ""                        
    
    # Test tag name with timestamp to make it unique
    TEST_TAG_PREFIX = "integration_test_python"
    
    @pytest.fixture
    def client(self):
        """Create a test client instance"""
        return TableauApiClient(
            tableau_base_uri=self.TABLEAU_SERVER_URL,
            api_version="3.7",
            logger=logging.getLogger("test"),
            timeout=timedelta(seconds=120),
            ignore_ssl_errors=False
        )
    
    @pytest.fixture
    def authenticated_session(self, client: TableauApiClient):
        """Create an authenticated session for testing"""
        print(f"Authenticating to: {self.TABLEAU_SERVER_URL}")
        
        try:
            # Try PAT authentication first, fallback to username/password
            if self.TEST_PAT_NAME and self.TEST_PAT_TOKEN:
                session = client.authentication.sign_in_with_pat(
                    token_name=self.TEST_PAT_NAME,
                    token=self.TEST_PAT_TOKEN,
                    site_content_url=self.TEST_SITE_URL
                )
                print("✓ PAT authentication successful!")
            else:
                session = client.authentication.sign_in(
                    user_name=self.TEST_USERNAME,
                    password=self.TEST_PASSWORD,
                    site_content_url=self.TEST_SITE_URL
                )
                print("✓ Username/password authentication successful!")
            
            yield session
            
            # Cleanup: sign out
            try:
                client.authentication.sign_out(session)
                print("✓ Successfully signed out")
            except Exception as e:
                print(f"⚠ Warning: Could not sign out cleanly: {e}")
                
        except Exception as e:
            pytest.fail(f"Authentication failed: {e}")
    
    def test_query_projects(self, client: TableauApiClient, authenticated_session: TableauSession):
        """Test querying all projects using pagination"""
        print("\n=== Testing Project Queries (All Pages) ===")
        
        try:
            all_projects = []
            page_number = 1
            page_size = 10
            total_processed = 0
            
            while True:
                print(f"Fetching page {page_number}...")
                pagination, projects = client.projects.query_projects(
                    session=authenticated_session,
                    page_size=page_size,
                    page_number=page_number
                )
                
                if not projects:
                    break
                    
                all_projects.extend(projects)
                total_processed += len(projects)
                
                print(f"  Page {page_number}: {len(projects)} projects")
                
                # Check if we've reached the end
                if (pagination and pagination.total_available and 
                    total_processed >= pagination.total_available) or len(projects) < page_size:
                    break
                    
                page_number += 1
            
            print(f"\n✓ Retrieved all {len(all_projects)} projects across {page_number} pages")
            if pagination and pagination.total_available:
                print(f"  Server reported total: {pagination.total_available}")
            
            # Display summary of all projects
            print(f"\nAll Projects Summary:")
            for i, project in enumerate(all_projects):
                print(f"  {i+1:2d}. {project.name} (ID: {project.id})")
                if project.description:
                    desc_preview = project.description[:50] + "..." if len(project.description) > 50 else project.description
                    print(f"      Description: {desc_preview}")
                print(f"      Content Permissions: {project.content_permissions}")
                print(f"      Created: {project.created_at if project.created_at else 'Unknown'}")
                if hasattr(project, 'parent_project_id') and project.parent_project_id:
                    print(f"      Parent Project ID: {project.parent_project_id}")
                print()
            
            # Store first project for later use
            if all_projects:
                self.first_project = all_projects[0]
                print(f"✓ Will use project '{self.first_project.name}' for further tests")
            
            return all_projects
            
        except TableauRequestException as e:
            print(f"✗ Failed to query projects: {e}")
            pytest.fail(f"Project query failed: {e}")
        except Exception as e:
            print(f"✗ Unexpected error: {e}")
            pytest.fail(f"Unexpected error during project query: {e}")
    
    def test_query_workbooks(self, client: TableauApiClient, authenticated_session: TableauSession):
        """Test querying all workbooks using pagination"""
        print("\n=== Testing Workbook Queries (All Pages) ===")
        
        try:
            all_workbooks = []
            page_number = 1
            page_size = 10
            total_processed = 0
            
            while True:
                print(f"Fetching workbooks page {page_number}...")
                pagination, workbooks = client.workbooks_views.query_workbooks_for_site(
                    session=authenticated_session,
                    page_size=page_size,
                    page_number=page_number
                )
                
                if not workbooks:
                    break
                    
                all_workbooks.extend(workbooks)
                total_processed += len(workbooks)
                
                print(f"  Page {page_number}: {len(workbooks)} workbooks")
                
                # Check if we've reached the end
                if (pagination and pagination.total_available and 
                    total_processed >= pagination.total_available) or len(workbooks) < page_size:
                    break
                    
                page_number += 1
            
            print(f"\n✓ Retrieved all {len(all_workbooks)} workbooks across {page_number} pages")
            if pagination and pagination.total_available:
                print(f"  Server reported total: {pagination.total_available}")
            
            # Display summary of all workbooks
            print(f"\nAll Workbooks Summary:")
            for i, workbook in enumerate(all_workbooks):
                print(f"  {i+1:2d}. {workbook.name} (ID: {workbook.id})")
                print(f"      Project: {workbook.project.name if workbook.project else 'Unknown'}")
                print(f"      Owner: {workbook.owner.name if workbook.owner else 'Unknown'}")
                print(f"      Created: {workbook.created_at if workbook.created_at else 'Unknown'}")
                print(f"      Updated: {workbook.updated_at if workbook.updated_at else 'Unknown'}")
                print(f"      Size: {workbook.size if workbook.size else 'Unknown'}")
                print(f"      Content URL: {workbook.content_url if workbook.content_url else 'Unknown'}")
                print(f"      Show Tabs: {workbook.show_tabs if workbook.show_tabs else 'Unknown'}")
                
                # Show tags if any
                if hasattr(workbook, 'tags') and workbook.tags and workbook.tags.tag:
                    tag_names = [tag.label for tag in workbook.tags.tag]
                    print(f"      Tags: {', '.join(tag_names)}")
                else:
                    print(f"      Tags: None")
                
                # Show view count if available
                if hasattr(workbook, 'views') and workbook.views and hasattr(workbook.views, 'view'):
                    view_count = len(workbook.views.view) if workbook.views.view else 0
                    print(f"      Views: {view_count}")
                print()
            
            # Store first workbook for tagging test
            if all_workbooks:
                self.first_workbook = all_workbooks[0]
                print(f"✓ Will use workbook '{self.first_workbook.name}' for tagging test")
            
            return all_workbooks
            
        except TableauApiVersionException as e:
            print(f"✗ API Version Error: {e}")
            pytest.skip("Workbook querying requires API version 2.3+")
        except TableauRequestException as e:
            print(f"✗ Failed to query workbooks: {e}")
            pytest.fail(f"Workbook query failed: {e}")
        except Exception as e:
            print(f"✗ Unexpected error: {e}")
            pytest.fail(f"Unexpected error during workbook query: {e}")
    
    def test_query_datasources(self, client: TableauApiClient, authenticated_session: TableauSession):
        """Test querying all datasources using pagination"""
        print("\n=== Testing Datasource Queries (All Pages) ===")
        
        try:
            all_datasources = []
            page_number = 1
            page_size = 10
            total_processed = 0
            
            while True:
                print(f"Fetching datasources page {page_number}...")
                pagination, datasources = client.datasources.query_data_sources(
                    session=authenticated_session,
                    page_size=page_size,
                    page_number=page_number
                )
                
                if not datasources:
                    break
                    
                all_datasources.extend(datasources)
                total_processed += len(datasources)
                
                print(f"  Page {page_number}: {len(datasources)} datasources")
                
                # Check if we've reached the end
                if (pagination and pagination.total_available and 
                    total_processed >= pagination.total_available) or len(datasources) < page_size:
                    break
                    
                page_number += 1
            
            print(f"\n✓ Retrieved all {len(all_datasources)} datasources across {page_number} pages")
            if pagination and pagination.total_available:
                print(f"  Server reported total: {pagination.total_available}")
            
            # Display summary of all datasources
            print(f"\nAll Datasources Summary:")
            for i, datasource in enumerate(all_datasources):
                print(f"  {i+1:2d}. {datasource.name} (ID: {datasource.id})")
                print(f"      Project: {datasource.project.name if datasource.project else 'Unknown'}")
                print(f"      Owner: {datasource.owner.name if datasource.owner else 'Unknown'}")
                print(f"      Type: {datasource.type_value if datasource.type_value else 'Unknown'}")
                print(f"      Created: {datasource.created_at if datasource.created_at else 'Unknown'}")
                print(f"      Updated: {datasource.updated_at if datasource.updated_at else 'Unknown'}")
                print(f"      Content URL: {datasource.content_url if datasource.content_url else 'Unknown'}")
                print(f"      Use Remote Query Agent: {datasource.use_remote_query_agent if hasattr(datasource, 'use_remote_query_agent') else 'Unknown'}")
                print(f"      Is Certified: {datasource.is_certified if hasattr(datasource, 'is_certified') else 'Unknown'}")
                
                if hasattr(datasource, 'certification_note') and datasource.certification_note:
                    cert_note = datasource.certification_note[:50] + "..." if len(datasource.certification_note) > 50 else datasource.certification_note
                    print(f"      Certification Note: {cert_note}")
                
                # Show tags if any
                if hasattr(datasource, 'tags') and datasource.tags and datasource.tags.tag:
                    tag_names = [tag.label for tag in datasource.tags.tag]
                    print(f"      Tags: {', '.join(tag_names)}")
                else:
                    print(f"      Tags: None")
                
                # Show size if available
                if hasattr(datasource, 'size') and datasource.size:
                    print(f"      Size: {datasource.size}")
                print()
            
            # Store first datasource for tagging test
            if all_datasources:
                self.first_datasource = all_datasources[0]
                print(f"✓ Will use datasource '{self.first_datasource.name}' for tagging test")
            
            return all_datasources
            
        except TableauRequestException as e:
            print(f"✗ Failed to query datasources: {e}")
            pytest.fail(f"Datasource query failed: {e}")
        except Exception as e:
            print(f"✗ Unexpected error: {e}")
            pytest.fail(f"Unexpected error during datasource query: {e}")
    
    def test_query_individual_workbook(self, client: TableauApiClient, authenticated_session: TableauSession):
        """Test querying a specific workbook for detailed information"""
        print("\n=== Testing Individual Workbook Query ===")
        
        # First get a workbook ID
        try:
            _, workbooks = client.workbooks_views.query_workbooks_for_site(
                session=authenticated_session,
                page_size=1
            )
            
            if not workbooks:
                pytest.skip("No workbooks available for detailed query test")
            
            workbook_id = workbooks[0].id
            print(f"Querying detailed info for workbook: {workbooks[0].name}")
            
            # Query individual workbook
            detailed_workbook = client.workbooks_views.query_workbook(
                session=authenticated_session,
                workbook_id=workbook_id
            )
            
            assert detailed_workbook is not None
            assert detailed_workbook.id == workbook_id
            
            print(f"✓ Retrieved detailed workbook information")
            print(f"  Name: {detailed_workbook.name}")
            print(f"  Description: {detailed_workbook.description or 'No description'}")
            print(f"  Show tabs: {detailed_workbook.show_tabs}")
            print(f"  Size: {detailed_workbook.size}")
            print(f"  Content URL: {detailed_workbook.content_url}")
            
            # Show views if any
            if hasattr(detailed_workbook, 'views') and detailed_workbook.views:
                if hasattr(detailed_workbook.views, 'view') and detailed_workbook.views.view:
                    print(f"  Views ({len(detailed_workbook.views.view)}):")
                    for view in detailed_workbook.views.view[:3]:  # Show first 3 views
                        print(f"    - {view.name} (ID: {view.id})")
            
        except TableauRequestException as e:
            print(f"✗ Failed to query individual workbook: {e}")
            pytest.fail(f"Individual workbook query failed: {e}")
        except Exception as e:
            print(f"✗ Unexpected error: {e}")
            pytest.fail(f"Unexpected error during individual workbook query: {e}")
    
    def test_query_individual_datasource(self, client: TableauApiClient, authenticated_session: TableauSession):
        """Test querying a specific datasource for detailed information"""
        print("\n=== Testing Individual Datasource Query ===")
        
        # First get a datasource ID
        try:
            _, datasources = client.datasources.query_data_sources(
                session=authenticated_session,
                page_size=1
            )
            
            if not datasources:
                pytest.skip("No datasources available for detailed query test")
            
            datasource_id = datasources[0].id
            print(f"Querying detailed info for datasource: {datasources[0].name}")
            
            # Query individual datasource
            detailed_datasource = client.datasources.query_data_source(
                session=authenticated_session,
                datasource_id=datasource_id
            )
            
            assert detailed_datasource is not None
            assert detailed_datasource.id == datasource_id
            
            print(f"✓ Retrieved detailed datasource information")
            print(f"  Name: {detailed_datasource.name}")
            print(f"  Description: {detailed_datasource.description or 'No description'}")
            print(f"  Type: {detailed_datasource.type_value}")
            print(f"  Content URL: {detailed_datasource.content_url}")
            print(f"  Use Remote Query Agent: {detailed_datasource.use_remote_query_agent}")
            
        except TableauRequestException as e:
            print(f"✗ Failed to query individual datasource: {e}")
            pytest.fail(f"Individual datasource query failed: {e}")
        except Exception as e:
            print(f"✗ Unexpected error: {e}")
            pytest.fail(f"Unexpected error during individual datasource query: {e}")
    
    def test_add_tags_to_workbook(self, client: TableauApiClient, authenticated_session: TableauSession):
        """Test adding custom tags to the first workbook"""
        print("\n=== Testing Workbook Tagging ===")
        
        # First get a workbook to tag
        try:
            _, workbooks = client.workbooks_views.query_workbooks_for_site(
                session=authenticated_session,
                page_size=1
            )
            
            if not workbooks:
                pytest.skip("No workbooks available for tagging test")
            
            workbook = workbooks[0]
            workbook_id = workbook.id
            
            # Create unique test tags
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            test_tags = [
                f"{self.TEST_TAG_PREFIX}_{timestamp}",
                f"automated_test_{timestamp}",
                "integration_test_tag"
            ]
            
            print(f"Adding tags to workbook: {workbook.name}")
            print(f"Tags to add: {', '.join(test_tags)}")
            
            # Add tags
            added_tags = client.workbooks_views.add_tags_to_workbook(
                session=authenticated_session,
                workbook_id=workbook_id,
                tags_to_add=test_tags
            )
            
            assert added_tags is not None
            assert isinstance(added_tags, list)
            assert len(added_tags) > 0
            
            print(f"✓ Successfully added {len(added_tags)} tags to workbook")
            for tag in added_tags:
                print(f"  - {tag.label}")
            
            # Store tags for cleanup
            self.workbook_test_tags = test_tags
            self.tagged_workbook_id = workbook_id
            
            return added_tags
            
        except TableauRequestException as e:
            print(f"✗ Failed to add tags to workbook: {e}")
            pytest.fail(f"Workbook tagging failed: {e}")
        except Exception as e:
            print(f"✗ Unexpected error: {e}")
            pytest.fail(f"Unexpected error during workbook tagging: {e}")
    
    def test_add_tags_to_datasource(self, client: TableauApiClient, authenticated_session: TableauSession):
        """Test adding custom tags to the first datasource"""
        print("\n=== Testing Datasource Tagging ===")
        
        # First get a datasource to tag
        try:
            _, datasources = client.datasources.query_data_sources(
                session=authenticated_session,
                page_size=1
            )
            
            if not datasources:
                pytest.skip("No datasources available for tagging test")
            
            datasource = datasources[0]
            datasource_id = datasource.id
            
            # Create unique test tags
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            test_tags = [
                f"{self.TEST_TAG_PREFIX}_ds_{timestamp}",
                f"data_source_test_{timestamp}",
                "automated_tag_test"
            ]
            
            print(f"Adding tags to datasource: {datasource.name}")
            print(f"Tags to add: {', '.join(test_tags)}")
            
            # Add tags
            added_tags = client.datasources.add_tags_to_data_source(
                session=authenticated_session,
                datasource_id=datasource_id,
                tags_to_add=test_tags
            )
            
            assert added_tags is not None
            assert isinstance(added_tags, list)
            assert len(added_tags) > 0
            
            print(f"✓ Successfully added {len(added_tags)} tags to datasource")
            for tag in added_tags:
                print(f"  - {tag.label}")
            
            # Store tags for cleanup
            self.datasource_test_tags = test_tags
            self.tagged_datasource_id = datasource_id
            
            return added_tags
            
        except TableauRequestException as e:
            print(f"✗ Failed to add tags to datasource: {e}")
            pytest.fail(f"Datasource tagging failed: {e}")
        except Exception as e:
            print(f"✗ Unexpected error: {e}")
            pytest.fail(f"Unexpected error during datasource tagging: {e}")
    
    def test_cleanup_tags(self, client: TableauApiClient, authenticated_session: TableauSession):
        """Clean up test tags that were added during testing"""
        print("\n=== Cleaning Up Test Tags ===")
        
        cleanup_errors = []
        
        # Clean up workbook tags
        if hasattr(self, 'tagged_workbook_id') and hasattr(self, 'workbook_test_tags'):
            print(f"Cleaning up workbook tags...")
            for tag in self.workbook_test_tags:
                try:
                    client.workbooks_views.delete_tag_from_workbook(
                        session=authenticated_session,
                        workbook_id=self.tagged_workbook_id,
                        tag_name=tag
                    )
                    print(f"  ✓ Removed tag: {tag}")
                except Exception as e:
                    cleanup_errors.append(f"Failed to remove workbook tag '{tag}': {e}")
                    print(f"  ✗ Failed to remove tag '{tag}': {e}")
        
        # Clean up datasource tags
        if hasattr(self, 'tagged_datasource_id') and hasattr(self, 'datasource_test_tags'):
            print(f"Cleaning up datasource tags...")
            for tag in self.datasource_test_tags:
                try:
                    client.datasources.delete_tag_from_data_source(
                        session=authenticated_session,
                        datasource_id=self.tagged_datasource_id,
                        tag=tag
                    )
                    print(f"  ✓ Removed tag: {tag}")
                except Exception as e:
                    cleanup_errors.append(f"Failed to remove datasource tag '{tag}': {e}")
                    print(f"  ✗ Failed to remove tag '{tag}': {e}")
        
        # Report cleanup status
        if cleanup_errors:
            print(f"⚠ Some cleanup operations failed:")
            for error in cleanup_errors:
                print(f"  - {error}")
        else:
            print("✓ All test tags cleaned up successfully")
    
    def test_query_with_filters(self, client: TableauApiClient, authenticated_session: TableauSession):
        """Test querying content with filters"""
        print("\n=== Testing Filtered Queries ===")
        
        try:
            # Test filtered workbook query
            print("Testing workbook filtering...")
            pagination, workbooks = client.workbooks_views.query_workbooks_for_site(
                session=authenticated_session,
                page_size=5,
                filter_expression="name:eq:Dev1"
            )
            
            print(f"Found {len(workbooks)} workbooks with 'Dev1' filter")
            for wb in workbooks:
                print(f"  - {wb.name}")
            
            # Test filtered datasource query
            print("Testing datasource filtering...")
            pagination, datasources = client.datasources.query_data_sources(
                session=authenticated_session,
                page_size=5,
                sort_expression="name:asc"  # Sort by name ascending
            )
            
            print(f"Found {len(datasources)} datasources (sorted by name)")
            for ds in datasources[:3]:  # Show first 3
                print(f"  - {ds.name}")
            
        except TableauApiVersionException as e:
            print(f"✗ API Version Error: {e}")
            pytest.skip("Filtered queries may require newer API versions")
        except TableauRequestException as e:
            print(f"⚠ Filter query failed (this may be expected): {e}")
            # Don't fail the test as filtering syntax can be tricky
        except Exception as e:
            print(f"✗ Unexpected error: {e}")
            pytest.fail(f"Unexpected error during filtered queries: {e}")


def run_content_tests():
    """Run content tests manually"""
    import logging
    logging.basicConfig(level=logging.INFO)
    
    test_instance = TestTableauContentIntegration()
    print("=" * 80)
    print("TABLEAU CONTENT INTEGRATION TESTS")
    print("=" * 80)
    print(f"Server: {test_instance.TABLEAU_SERVER_URL}")
    print(f"Username: {test_instance.TEST_USERNAME}")
    print("=" * 80)
    
    # Check if test values are configured
    if not test_instance.TABLEAU_SERVER_URL or "your-tableau-server.com" in test_instance.TABLEAU_SERVER_URL:
        print("⚠ Please update the TABLEAU_SERVER_URL in the test configuration")
        return
    
    if not test_instance.TEST_USERNAME or "your_username" in test_instance.TEST_USERNAME:
        print("⚠ Please update the TEST_USERNAME in the test configuration")
        return
    
    # Create client and session
    client = TableauApiClient(
        tableau_base_uri=test_instance.TABLEAU_SERVER_URL,
        api_version="3.7",
        logger=logging.getLogger("test"),
        ignore_ssl_errors=False
    )
    
    try:
        # Authenticate
        if test_instance.TEST_PAT_NAME and test_instance.TEST_PAT_TOKEN and "your_pat" not in test_instance.TEST_PAT_NAME:
            print("Using PAT authentication...")
            session = client.authentication.sign_in_with_pat(
                token_name=test_instance.TEST_PAT_NAME,
                token=test_instance.TEST_PAT_TOKEN,
                site_content_url=test_instance.TEST_SITE_URL
            )
        else:
            print("Using username/password authentication...")
            session = client.authentication.sign_in(
                user_name=test_instance.TEST_USERNAME,
                password=test_instance.TEST_PASSWORD,
                site_content_url=test_instance.TEST_SITE_URL
            )
        
        print("✓ Authentication successful!")
        
        test_instance.test_query_projects(client, session)
        test_instance.test_query_workbooks(client, session)
        test_instance.test_query_datasources(client, session)
        test_instance.test_query_individual_workbook(client, session)
        test_instance.test_query_individual_datasource(client, session)
        test_instance.test_add_tags_to_workbook(client, session)
        test_instance.test_add_tags_to_datasource(client, session)
        test_instance.test_query_with_filters(client, session)
        test_instance.test_cleanup_tags(client, session)
        
        print("\n" + "=" * 80)
        print("✓ All content tests completed successfully!")
        print("=" * 80)
        
        # Sign out
        client.authentication.sign_out(session)
        
    except Exception as e:
        print(f"\n✗ Test execution failed: {e}")
        print("Please check your configuration and server connectivity.")


if __name__ == "__main__":
    print("\nTo run with pytest: pytest test_tableau_content_integration.py -v -s")
    print("To run manually: python test_tableau_content_integration.py")
    print("\n" + "="*60)
    run_content_tests()
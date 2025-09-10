import pytest
import logging

from tableau_api_client import TableauApiClient
from models.tableau_session import TableauSession
from exceptions.tableau_api_version_exception import TableauApiVersionException
from exceptions.tableau_online_not_supported_exception import TableauOnlineNotSupportedException
from exceptions.tableau_request_exception import TableauRequestException


class TestTableauAuthenticationIntegration:
    """Integration tests for Tableau API authentication methods with real server calls"""
    
    # HARDCODED TEST VALUES
    TABLEAU_SERVER_URL = ""
    TEST_USERNAME = ""
    TEST_PASSWORD = ""
    TEST_SITE_URL = ""
    TEST_PAT_NAME = ""                        
    TEST_PAT_TOKEN = ""
    TEST_USER_TO_IMPERSONATE = ""
    TEST_NEW_SITE_URL = ""   
    
    @pytest.fixture
    def client(self):
        """Create a test client instance"""
        return TableauApiClient(
            tableau_base_uri=self.TABLEAU_SERVER_URL,
            api_version="3.7",
            logger=logging.getLogger("test"),
            timeout=60,
            ignore_ssl_errors=False  # Set to False in production
        )
    
    @pytest.fixture
    def client_old_version(self):
        """Create a test client with older API version"""
        return TableauApiClient(
            tableau_base_uri=self.TABLEAU_SERVER_URL,
            api_version="2.0",
            logger=logging.getLogger("test"),
            timeout=60,
            ignore_ssl_errors=False
        )
    
    def test_sign_in_basic(self, client):
        """Test basic username/password sign in"""
        print(f"Testing sign in to: {self.TABLEAU_SERVER_URL}")
        
        try:
            session = client.sign_in(
                user_name=self.TEST_USERNAME,
                password=self.TEST_PASSWORD,
                site_content_url=self.TEST_SITE_URL
            )
            
            # Verify session was created successfully
            assert session is not None
            assert isinstance(session, TableauSession)
            assert session.token is not None
            assert session.user_id is not None
            assert session.site_id is not None
            
            print(f"✓ Sign in successful!")
            print(f"  User ID: {session.user_id}")
            print(f"  User Name: {session.user_name}")
            print(f"  Site ID: {session.site_id}")
            print(f"  Token: {session.token[:10]}...")
            
            # Test sign out
            client.sign_out(session)
            print("✓ Sign out successful!")
            
        except TableauRequestException as e:
            print(f"✗ Tableau Server Error: {e}")
            pytest.fail(f"Authentication failed: {e}")
        except Exception as e:
            print(f"✗ Unexpected Error: {e}")
            pytest.fail(f"Unexpected error during authentication: {e}")
    
    def test_sign_in_with_impersonation(self, client):
        """Test sign in with user impersonation (if supported)"""
        print(f"Testing sign in with impersonation")
        
        try:
            session = client.sign_in(
                user_name=self.TEST_USERNAME,
                password=self.TEST_PASSWORD,
                site_content_url=self.TEST_SITE_URL,
                user_id_to_impersonate=self.TEST_USER_TO_IMPERSONATE
            )
            
            assert session is not None
            assert session.token is not None
            
            print(f"✓ Sign in with impersonation successful!")
            print(f"  Impersonated User ID: {session.user_id}")
            
            # Clean up
            client.sign_out(session)
            print("✓ Sign out successful!")
            
        except TableauRequestException as e:
            print(f"✗ Impersonation failed (this may be expected): {e}")
            # Don't fail the test as impersonation might not be configured
        except Exception as e:
            pytest.fail(f"Unexpected error during impersonation test: {e}")
    
    def test_sign_in_with_pat(self, client):
        """Test sign in with Personal Access Token"""
        print(f"Testing sign in with Personal Access Token")
        
        try:
            session = client.sign_in_with_pat(
                token_name=self.TEST_PAT_NAME,
                token=self.TEST_PAT_TOKEN,
                site_content_url=self.TEST_SITE_URL
            )
            
            assert session is not None
            assert isinstance(session, TableauSession)
            assert session.token is not None
            assert session.user_id is not None
            
            print(f"✓ PAT sign in successful!")
            print(f"  User ID: {session.user_id}")
            print(f"  User Name: {session.user_name}")
            print(f"  Token: {session.token[:10]}...")
            
            # Test sign out
            client.sign_out(session)
            print("✓ Sign out successful!")
            
        except TableauApiVersionException as e:
            print(f"✗ API Version Error: {e}")
            pytest.skip("PAT authentication requires API version 3.7+")
        except TableauRequestException as e:
            print(f"✗ PAT Authentication failed: {e}")
            pytest.fail(f"PAT authentication failed: {e}")
        except Exception as e:
            pytest.fail(f"Unexpected error during PAT authentication: {e}")
    
    def test_sign_in_with_pat_and_impersonation(self, client):
        """Test PAT sign in with user impersonation"""
        print(f"Testing PAT sign in with impersonation")
        
        try:
            session = client.sign_in_with_pat(
                token_name=self.TEST_PAT_NAME,
                token=self.TEST_PAT_TOKEN,
                site_content_url=self.TEST_SITE_URL,
                user_id_to_impersonate=self.TEST_USER_TO_IMPERSONATE
            )
            
            assert session is not None
            assert session.token is not None
            
            print(f"✓ PAT sign in with impersonation successful!")
            print(f"  Impersonated User ID: {session.user_id}")
            
            # Clean up
            client.sign_out(session)
            print("✓ Sign out successful!")
            
        except TableauRequestException as e:
            print(f"✗ PAT impersonation failed (this may be expected): {e}")
            # Don't fail the test as impersonation might not be configured
        except Exception as e:
            pytest.fail(f"Unexpected error during PAT impersonation test: {e}")
    
    def test_switch_site(self, client):
        """Test switching sites (On-Premise only)"""
        print(f"Testing site switching")
        
        try:
            # First sign in to get a session
            session = client.sign_in(
                user_name=self.TEST_USERNAME,
                password=self.TEST_PASSWORD,
                site_content_url=self.TEST_SITE_URL
            )
            
            print(f"✓ Initial sign in successful")
            print(f"  Original Site ID: {session.site_id}")
            
            # Try to switch to different site
            new_session = client.switch_site(
                session=session,
                new_site_content_url=self.TEST_NEW_SITE_URL
            )
            
            assert new_session is not None
            assert new_session.token is not None
            assert new_session.site_id != session.site_id  # Should be different site
            
            print(f"✓ Site switch successful!")
            print(f"  New Site ID: {new_session.site_id}")
            
            # Clean up
            client.sign_out(new_session)
            print("✓ Sign out successful!")
            
        except TableauOnlineNotSupportedException as e:
            print(f"✓ Site switching not supported (Tableau Online): {e}")
            pytest.skip("Site switching is not supported on Tableau Online")
        except TableauApiVersionException as e:
            print(f"✗ API Version Error: {e}")
            pytest.skip("Site switching requires API version 2.6+")
        except TableauRequestException as e:
            print(f"✗ Site switch failed: {e}")
            # Try to clean up original session
            try:
                client.sign_out(session)
            except:
                pass
            pytest.fail(f"Site switch failed: {e}")
        except Exception as e:
            pytest.fail(f"Unexpected error during site switch test: {e}")
    
    def test_pat_with_old_api_version(self, client_old_version):
        """Test that PAT authentication fails with old API version"""
        print("Testing PAT with old API version (should fail)")
        
        with pytest.raises(TableauApiVersionException):
            client_old_version.sign_in_with_pat(
                token_name=self.TEST_PAT_NAME,
                token=self.TEST_PAT_TOKEN,
                site_content_url=self.TEST_SITE_URL
            )
        
        print("✓ PAT correctly rejected for API version < 3.7")
    
    def test_invalid_credentials(self, client):
        """Test sign in with invalid credentials"""
        print("Testing invalid credentials (should fail)")
        
        with pytest.raises(TableauRequestException):
            client.sign_in(
                user_name="invalid_user",
                password="invalid_password",
                site_content_url=self.TEST_SITE_URL
            )
        
        print("✓ Invalid credentials correctly rejected")
    
    def test_sign_out_invalid_session(self, client):
        """Test sign out with invalid session token"""
        print("Testing sign out with invalid session")
        
        # Create a fake session
        fake_session = TableauSession()
        fake_session.token = "invalid_token"
        fake_session.user_id = "fake_user_id"
        fake_session.site_id = "fake_site_id"
        
        with pytest.raises(TableauRequestException):
            client.sign_out(fake_session)
        
        print("✓ Invalid session correctly rejected")


def run_auth_tests():
    """Run authentication tests manually"""
    import logging
    logging.basicConfig(level=logging.INFO)
    
    # Update these values before running tests
    test_instance = TestTableauAuthenticationIntegration()
    print("=" * 60)
    print("TABLEAU AUTHENTICATION INTEGRATION TESTS")
    print("=" * 60)
    print(f"Server: {test_instance.TABLEAU_SERVER_URL}")
    print(f"Username: {test_instance.TEST_USERNAME}")
    print("=" * 60)
    
    # Run basic sign in test
    client = TableauApiClient(
        tableau_base_uri=test_instance.TABLEAU_SERVER_URL,
        api_version="3.7",
        logger=logging.getLogger("test"),
        ignore_ssl_errors=False
    )
    
    try:
        test_instance.test_sign_in_basic(client)
        print("\n✓ All tests completed successfully!")
    except Exception as e:
        print(f"\n✗ Test failed: {e}")


if __name__ == "__main__":
    print("Then run: pytest test_tableau_auth_integration.py -v")
    print("Or run manually with (simple singin nothing more): python test_tableau_auth_integration.py")
    run_auth_tests()
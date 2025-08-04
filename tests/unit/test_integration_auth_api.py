"""Test cases for the integration auth API endpoints."""

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from openhands.server.app import app
from openhands.server.routes.integration.auth import GrantType


@pytest.fixture
def test_client():
    """Create a test client for the FastAPI app."""
    return TestClient(app)


@pytest.fixture
def mock_auth_middleware():
    """Mock the JWT authentication middleware."""
    with patch('openhands.server.middleware.JWTAuthMiddleware.dispatch') as mock_auth:

        async def mock_auth_dispatch(request, call_next):
            # Simulate authenticated request
            request.state.user_id = 'test-user-id'
            return await call_next(request)

        mock_auth.side_effect = mock_auth_dispatch
        yield mock_auth


class TestIntegrationAuthAPI:
    """Test cases for the integration auth API endpoints."""

    @pytest.mark.asyncio
    async def test_get_token_with_api_key_success(
        self, test_client, mock_auth_middleware
    ):
        """Test successful token generation with API key."""
        with patch(
            'openhands.server.routes.integration.auth.generate_access_token_from_api_key'
        ) as mock_generate_api_key:
            # Mock successful response
            mock_generate_api_key.return_value = {
                'accessToken': 'test-access-token',
                'refreshToken': 'test-refresh-token',
            }

            payload = {'grant_type': 'api_key', 'api_key': 'test-api-key-123'}

            response = test_client.post('/api/v1/integration/auth/token', json=payload)

            assert response.status_code == 200
            data = response.json()
            assert data['message'] == 'Get accesstoken successfully'
            assert data['access_token'] == 'test-access-token'
            assert data['refresh_token'] == 'test-refresh-token'

            # Verify the function was called with correct arguments
            mock_generate_api_key.assert_called_once_with('test-api-key-123')

    @pytest.mark.asyncio
    async def test_get_token_with_refresh_token_success(
        self, test_client, mock_auth_middleware
    ):
        """Test successful token generation with refresh token."""
        with patch(
            'openhands.server.routes.integration.auth.generate_access_token_from_refresh_token'
        ) as mock_generate_refresh:
            # Mock successful response
            mock_generate_refresh.return_value = {
                'accessToken': 'new-access-token',
                'refreshToken': 'new-refresh-token',
            }

            payload = {
                'grant_type': 'refresh_token',
                'refresh_token': 'old-refresh-token-123',
            }

            response = test_client.post('/api/v1/integration/auth/token', json=payload)

            assert response.status_code == 200
            data = response.json()
            assert data['message'] == 'Get accesstoken successfully'
            assert data['access_token'] == 'new-access-token'
            assert data['refresh_token'] == 'new-refresh-token'

            # Verify the function was called with correct arguments
            mock_generate_refresh.assert_called_once_with('old-refresh-token-123')

    @pytest.mark.asyncio
    async def test_get_token_invalid_api_key(self, test_client, mock_auth_middleware):
        """Test token generation with invalid API key."""
        with patch(
            'openhands.server.routes.integration.auth.generate_access_token_from_api_key'
        ) as mock_generate_api_key:
            # Mock response without accessToken (invalid)
            mock_generate_api_key.return_value = {'error': 'Invalid API key'}

            payload = {'grant_type': 'api_key', 'api_key': 'invalid-api-key'}

            response = test_client.post('/api/v1/integration/auth/token', json=payload)

            assert response.status_code == 401
            data = response.json()
            assert data['detail'] == 'Invalid API key or refresh token'

    @pytest.mark.asyncio
    async def test_get_token_invalid_refresh_token(
        self, test_client, mock_auth_middleware
    ):
        """Test token generation with invalid refresh token."""
        with patch(
            'openhands.server.routes.integration.auth.generate_access_token_from_refresh_token'
        ) as mock_generate_refresh:
            # Mock response without accessToken (invalid)
            mock_generate_refresh.return_value = {'error': 'Invalid refresh token'}

            payload = {
                'grant_type': 'refresh_token',
                'refresh_token': 'invalid-refresh-token',
            }

            response = test_client.post('/api/v1/integration/auth/token', json=payload)

            assert response.status_code == 401
            data = response.json()
            assert data['detail'] == 'Invalid API key or refresh token'

    @pytest.mark.asyncio
    async def test_get_token_missing_api_key(self, test_client, mock_auth_middleware):
        """Test validation error when API key is missing."""
        payload = {
            'grant_type': 'api_key'
            # Missing api_key field
        }

        response = test_client.post('/api/v1/integration/auth/token', json=payload)

        assert response.status_code == 422
        data = response.json()
        assert 'detail' in data
        # Should contain validation error about missing api_key

    @pytest.mark.asyncio
    async def test_get_token_missing_refresh_token(
        self, test_client, mock_auth_middleware
    ):
        """Test validation error when refresh token is missing."""
        payload = {
            'grant_type': 'refresh_token'
            # Missing refresh_token field
        }

        response = test_client.post('/api/v1/integration/auth/token', json=payload)

        assert response.status_code == 422
        data = response.json()
        assert 'detail' in data
        # Should contain validation error about missing refresh_token

    @pytest.mark.asyncio
    async def test_get_token_invalid_grant_type(
        self, test_client, mock_auth_middleware
    ):
        """Test validation error with invalid grant type."""
        payload = {'grant_type': 'invalid_type', 'api_key': 'test-api-key'}

        response = test_client.post('/api/v1/integration/auth/token', json=payload)

        assert response.status_code == 422
        data = response.json()
        assert 'detail' in data

    @pytest.mark.asyncio
    async def test_get_token_empty_api_key(self, test_client, mock_auth_middleware):
        """Test validation error with empty API key."""
        payload = {'grant_type': 'api_key', 'api_key': ''}

        response = test_client.post('/api/v1/integration/auth/token', json=payload)

        assert response.status_code == 422
        data = response.json()
        assert 'detail' in data
        # Should contain validation error about empty api_key

    @pytest.mark.asyncio
    async def test_get_token_empty_refresh_token(
        self, test_client, mock_auth_middleware
    ):
        """Test validation error with empty refresh token."""
        payload = {'grant_type': 'refresh_token', 'refresh_token': ''}

        response = test_client.post('/api/v1/integration/auth/token', json=payload)

        assert response.status_code == 422
        data = response.json()
        assert 'detail' in data
        # Should contain validation error about empty refresh_token

    @pytest.mark.asyncio
    async def test_get_token_null_api_key_response(
        self, test_client, mock_auth_middleware
    ):
        """Test 401 response when API key function returns null access token."""
        with patch(
            'openhands.server.routes.integration.auth.generate_access_token_from_api_key'
        ) as mock_generate_api_key:
            # Mock response with null/None accessToken
            mock_generate_api_key.return_value = {
                'accessToken': None,
                'refreshToken': 'some-refresh-token',
            }

            payload = {
                'grant_type': 'api_key',
                'api_key': 'invalid-but-valid-format-key',
            }

            response = test_client.post('/api/v1/integration/auth/token', json=payload)

            assert response.status_code == 401
            data = response.json()
            assert data['detail'] == 'Invalid API key or refresh token'

    @pytest.mark.asyncio
    async def test_get_token_null_refresh_token_response(
        self, test_client, mock_auth_middleware
    ):
        """Test 401 response when refresh token function returns null access token."""
        with patch(
            'openhands.server.routes.integration.auth.generate_access_token_from_refresh_token'
        ) as mock_generate_refresh:
            # Mock response with null/None accessToken
            mock_generate_refresh.return_value = {
                'accessToken': None,
                'refreshToken': 'some-refresh-token',
            }

            payload = {
                'grant_type': 'refresh_token',
                'refresh_token': 'invalid-but-valid-format-token',
            }

            response = test_client.post('/api/v1/integration/auth/token', json=payload)

            assert response.status_code == 401
            data = response.json()
            assert data['detail'] == 'Invalid API key or refresh token'

    @pytest.mark.asyncio
    async def test_get_token_both_credentials_provided(
        self, test_client, mock_auth_middleware
    ):
        """Test that only the relevant credential is used based on grant_type."""
        with patch(
            'openhands.server.routes.integration.auth.generate_access_token_from_api_key'
        ) as mock_generate_api_key:
            # Mock successful response
            mock_generate_api_key.return_value = {
                'accessToken': 'test-access-token',
                'refreshToken': 'test-refresh-token',
            }

            payload = {
                'grant_type': 'api_key',
                'api_key': 'test-api-key',
                'refresh_token': 'test-refresh-token',  # This should be ignored
            }

            response = test_client.post('/api/v1/integration/auth/token', json=payload)

            assert response.status_code == 200
            # Should only call the API key function, not refresh token
            mock_generate_api_key.assert_called_once_with('test-api-key')

    @pytest.mark.asyncio
    async def test_get_token_default_grant_type(
        self, test_client, mock_auth_middleware
    ):
        """Test that default grant type is api_key."""
        with patch(
            'openhands.server.routes.integration.auth.generate_access_token_from_api_key'
        ) as mock_generate_api_key:
            # Mock successful response
            mock_generate_api_key.return_value = {
                'accessToken': 'test-access-token',
                'refreshToken': 'test-refresh-token',
            }

            payload = {
                # No grant_type specified, should default to api_key
                'api_key': 'test-api-key'
            }

            response = test_client.post('/api/v1/integration/auth/token', json=payload)

            assert response.status_code == 200
            mock_generate_api_key.assert_called_once_with('test-api-key')

    @pytest.mark.asyncio
    async def test_get_token_response_format(self, test_client, mock_auth_middleware):
        """Test the response format matches the expected schema."""
        with patch(
            'openhands.server.routes.integration.auth.generate_access_token_from_api_key'
        ) as mock_generate_api_key:
            # Mock successful response
            mock_generate_api_key.return_value = {
                'accessToken': 'test-access-token-12345',
                'refreshToken': 'test-refresh-token-67890',
            }

            payload = {'grant_type': 'api_key', 'api_key': 'test-api-key'}

            response = test_client.post('/api/v1/integration/auth/token', json=payload)

            assert response.status_code == 200
            data = response.json()

            # Verify all required fields are present
            assert 'message' in data
            assert 'access_token' in data
            assert 'refresh_token' in data

            # Verify field types
            assert isinstance(data['message'], str)
            assert isinstance(data['access_token'], str)
            assert isinstance(data['refresh_token'], str)

            # Verify specific values
            assert data['message'] == 'Get accesstoken successfully'
            assert data['access_token'] == 'test-access-token-12345'
            assert data['refresh_token'] == 'test-refresh-token-67890'


class TestGrantTypeEnum:
    """Test cases for the GrantType enum."""

    def test_grant_type_values(self):
        """Test that GrantType enum has correct values."""
        assert GrantType.API_KEY.value == 'api_key'
        assert GrantType.REFRESH_TOKEN.value == 'refresh_token'

    def test_grant_type_string_representation(self):
        """Test string representation of GrantType enum."""
        assert str(GrantType.API_KEY.value) == 'api_key'
        assert str(GrantType.REFRESH_TOKEN.value) == 'refresh_token'


class TestAuthRequestValidation:
    """Test cases for request validation logic."""

    @pytest.mark.asyncio
    async def test_request_validation_api_key_provided(self):
        """Test that validation passes when api_key is provided for api_key grant_type."""
        from openhands.server.routes.integration.auth import GetAccessTokenRequest

        request = GetAccessTokenRequest(
            grant_type=GrantType.API_KEY, api_key='test-api-key'
        )

        assert request.grant_type == GrantType.API_KEY
        assert request.api_key == 'test-api-key'
        assert request.refresh_token is None

    @pytest.mark.asyncio
    async def test_request_validation_refresh_token_provided(self):
        """Test that validation passes when refresh_token is provided for refresh_token grant_type."""
        from openhands.server.routes.integration.auth import GetAccessTokenRequest

        request = GetAccessTokenRequest(
            grant_type=GrantType.REFRESH_TOKEN, refresh_token='test-refresh-token'
        )

        assert request.grant_type == GrantType.REFRESH_TOKEN
        assert request.refresh_token == 'test-refresh-token'
        assert request.api_key is None

    @pytest.mark.asyncio
    async def test_request_validation_api_key_missing(self):
        """Test that validation fails when api_key is missing for api_key grant_type."""
        from pydantic import ValidationError

        from openhands.server.routes.integration.auth import GetAccessTokenRequest

        with pytest.raises(ValidationError) as exc_info:
            GetAccessTokenRequest(
                grant_type=GrantType.API_KEY
                # Missing api_key
            )

        assert 'api_key is required' in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_request_validation_refresh_token_missing(self):
        """Test that validation fails when refresh_token is missing for refresh_token grant_type."""
        from pydantic import ValidationError

        from openhands.server.routes.integration.auth import GetAccessTokenRequest

        with pytest.raises(ValidationError) as exc_info:
            GetAccessTokenRequest(
                grant_type=GrantType.REFRESH_TOKEN
                # Missing refresh_token
            )

        assert 'refresh_token is required' in str(exc_info.value)

from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from openhands.server.app import app


@pytest.fixture
def test_client():
    return TestClient(app)


@pytest.fixture
def mock_space_list_response():
    """Mock response for get_list_space."""
    return (
        [
            {
                'id': '1',
                'spaceId': '1',
                'userId': '123',
                'space': {
                    'id': '1',
                    'userId': '123',
                    'title': 'Test Space 1',
                    'description': 'A test space for development',
                    'createdAt': '2025-01-01T00:00:00Z',
                    'updatedAt': '2025-01-01T12:00:00Z',
                },
            },
            {
                'id': '2',
                'spaceId': '2',
                'userId': '123',
                'space': {
                    'id': '2',
                    'userId': '123',
                    'title': 'Test Space 2',
                    'description': 'Another test space',
                    'createdAt': '2025-01-01T01:00:00Z',
                    'updatedAt': '2025-01-01T13:00:00Z',
                },
            },
        ],
        {
            'total': 2,
            'offset': 0,
            'limit': 10,
            'has_more': False,
        },
    )


@pytest.fixture
def mock_space_detail_response():
    """Mock response for get_space_detail."""
    return {
        'id': '1',
        'userId': '123',
        'title': 'Test Space 1',
        'description': 'A test space for development',
        'createdAt': '2025-01-01T00:00:00Z',
        'updatedAt': '2025-01-01T12:00:00Z',
        'user': {
            'id': '123',
            'username': 'Test User',
        },
        'memberCount': 5,
    }


@pytest.fixture
def mock_space_sections_response():
    """Mock response for get_list_sections."""
    return [
        {
            'id': '101',
            'spaceId': '1',
            'name': 'Section 1',
            'description': 'First section',
            'conversationId': 'conv_101',
            'createdAt': '2025-01-01T00:00:00Z',
        },
        {
            'id': '102',
            'spaceId': '1',
            'name': 'Section 2',
            'description': 'Second section',
            'conversationId': 'conv_102',
            'createdAt': '2025-01-01T01:00:00Z',
        },
    ]


class TestIntegrationSpaceAPI:
    """Test cases for the integration space API endpoints."""

    def test_get_list_space_success(self, test_client, mock_space_list_response):
        """Test successful space list retrieval."""
        with patch(
            'openhands.server.routes.integration.space.SpaceModule'
        ) as mock_space_module_class:
            # Mock SpaceModule instance and its methods
            mock_space_module = mock_space_module_class.return_value
            mock_space_module.get_list_space = AsyncMock(
                return_value=mock_space_list_response
            )

            response = test_client.get(
                '/api/v1/integration/spaces',
                headers={'Authorization': 'Bearer test-token'},
            )

            assert response.status_code == 200
            data = response.json()

            # Verify response structure
            assert 'data' in data
            assert 'pagination' in data
            assert 'status' in data
            assert data['status'] == 'Get list spaces success'

            # Verify data content
            assert len(data['data']) == 2
            assert data['data'][0]['id'] == '1'
            assert data['data'][0]['space']['title'] == 'Test Space 1'
            assert data['data'][1]['id'] == '2'
            assert data['data'][1]['space']['title'] == 'Test Space 2'

            # Verify pagination
            assert data['pagination']['total'] == 2
            assert data['pagination']['offset'] == 0
            assert data['pagination']['limit'] == 10
            assert data['pagination']['has_more'] is False

            # Verify the function was called with correct parameters
            mock_space_module.get_list_space.assert_called_once_with(0, 10, None)

    def test_get_list_space_with_filters(self, test_client, mock_space_list_response):
        """Test space list retrieval with filters."""
        with patch(
            'openhands.server.routes.integration.space.SpaceModule'
        ) as mock_space_module_class:
            # Filter response for search
            filtered_response = (
                [mock_space_list_response[0][0]],
                {
                    'total': 1,
                    'offset': 5,
                    'limit': 5,
                    'has_more': False,
                },
            )
            mock_space_module = mock_space_module_class.return_value
            mock_space_module.get_list_space = AsyncMock(return_value=filtered_response)

            response = test_client.get(
                '/api/v1/integration/spaces?offset=5&limit=5&title=Test%20Space%201',
                headers={'Authorization': 'Bearer test-token'},
            )

            assert response.status_code == 200
            data = response.json()

            # Verify filtered results
            assert len(data['data']) == 1
            assert data['data'][0]['space']['title'] == 'Test Space 1'

            # Verify pagination reflects filters
            assert data['pagination']['offset'] == 5
            assert data['pagination']['limit'] == 5

            # Verify the function was called with filters
            mock_space_module.get_list_space.assert_called_once_with(
                5, 5, 'Test Space 1'
            )

    def test_get_list_space_empty_result(self, test_client):
        """Test space list retrieval with empty result."""
        with patch(
            'openhands.server.routes.integration.space.SpaceModule'
        ) as mock_space_module_class:
            # Mock empty response
            mock_space_module = mock_space_module_class.return_value
            mock_space_module.get_list_space = AsyncMock(return_value=([], None))

            response = test_client.get(
                '/api/v1/integration/spaces',
                headers={'Authorization': 'Bearer test-token'},
            )

            assert response.status_code == 200
            data = response.json()

            # Should return empty list when no data
            assert data['data'] == []
            assert data['status'] == 'Get list spaces success'

    def test_get_list_space_unauthorized(self, test_client):
        """Test space list retrieval with unauthorized token."""
        with patch(
            'openhands.server.routes.integration.space.SpaceModule'
        ) as mock_space_module_class:
            mock_space_module = mock_space_module_class.return_value
            mock_space_module.get_list_space = AsyncMock(
                side_effect=HTTPException(status_code=401, detail='Unauthorized')
            )

            response = test_client.get(
                '/api/v1/integration/spaces',
                headers={'Authorization': 'Bearer invalid-token'},
            )

            assert response.status_code == 401
            assert response.json()['detail'] == 'Unauthorized'

    def test_get_space_detail_success(self, test_client, mock_space_detail_response):
        """Test successful space detail retrieval."""
        space_id = '1'

        with patch(
            'openhands.server.routes.integration.space.SpaceModule'
        ) as mock_space_module_class:
            mock_space_module = mock_space_module_class.return_value
            mock_space_module.get_space_detail = AsyncMock(
                return_value=mock_space_detail_response
            )

            response = test_client.get(
                f'/api/v1/integration/spaces/{space_id}',
                headers={'Authorization': 'Bearer test-token'},
            )

            assert response.status_code == 200
            data = response.json()

            # Verify response structure
            assert 'data' in data
            assert 'status' in data
            assert data['status'] == 'Get space detail success'

            # Verify data content
            space_data = data['data']
            assert space_data['id'] == '1'
            assert space_data['title'] == 'Test Space 1'
            assert space_data['description'] == 'A test space for development'
            assert 'user' in space_data
            assert space_data['user']['username'] == 'Test User'
            assert space_data['memberCount'] == 5

            # Verify the function was called with correct parameters
            mock_space_module.get_space_detail.assert_called_once_with(space_id)

    def test_get_space_detail_not_found(self, test_client):
        """Test space detail retrieval for non-existent space."""
        space_id = '999'

        with patch(
            'openhands.server.routes.integration.space.SpaceModule'
        ) as mock_space_module_class:
            mock_space_module = mock_space_module_class.return_value
            mock_space_module.get_space_detail = AsyncMock(
                side_effect=HTTPException(status_code=404, detail='Space not found')
            )

            response = test_client.get(
                f'/api/v1/integration/spaces/{space_id}',
                headers={'Authorization': 'Bearer test-token'},
            )

            assert response.status_code == 404
            assert response.json()['detail'] == 'Space not found'

    def test_get_space_detail_empty_data(self, test_client):
        """Test space detail retrieval with empty data response."""
        space_id = '1'

        with patch(
            'openhands.server.routes.integration.space.SpaceModule'
        ) as mock_space_module_class:
            # Mock SpaceModule instance and its methods
            mock_space_module = mock_space_module_class.return_value
            mock_space_module.get_space_detail = AsyncMock(return_value=None)

            response = test_client.get(
                f'/api/v1/integration/spaces/{space_id}',
                headers={'Authorization': 'Bearer test-token'},
            )

            # When SpaceModule returns None, the API should return the None data
            assert response.status_code == 200
            data = response.json()
            assert data['data'] is None
            assert data['status'] == 'Get space detail success'

    def test_get_space_sections_success(
        self, test_client, mock_space_detail_response, mock_space_sections_response
    ):
        """Test successful space sections retrieval."""
        space_id = '1'

        with patch(
            'openhands.server.routes.integration.space.SpaceModule'
        ) as mock_space_module_class:
            mock_space_module = mock_space_module_class.return_value
            mock_space_module.get_space_detail = AsyncMock(
                return_value=mock_space_detail_response
            )
            mock_space_module.get_list_sections = AsyncMock(
                return_value=mock_space_sections_response
            )

            response = test_client.get(
                f'/api/v1/integration/spaces/{space_id}/sections',
                headers={'Authorization': 'Bearer test-token'},
            )

            assert response.status_code == 200
            data = response.json()

            # Verify response structure
            assert 'data' in data
            assert 'status' in data
            assert data['status'] == 'Get space sections success'

            # Verify data content
            sections = data['data']
            assert len(sections) == 2
            assert sections[0]['id'] == '101'
            assert sections[0]['name'] == 'Section 1'
            assert sections[0]['spaceId'] == '1'
            assert sections[1]['id'] == '102'
            assert sections[1]['name'] == 'Section 2'
            assert sections[1]['spaceId'] == '1'

            # Verify both functions were called
            mock_space_module.get_space_detail.assert_called_once_with(space_id)
            mock_space_module.get_list_sections.assert_called_once_with(space_id)

    def test_get_space_sections_space_not_found(self, test_client):
        """Test space sections retrieval when space doesn't exist."""
        space_id = '999'

        with patch(
            'openhands.server.routes.integration.space.SpaceModule'
        ) as mock_space_module_class:
            mock_space_module = mock_space_module_class.return_value
            mock_space_module.get_space_detail = AsyncMock(
                side_effect=HTTPException(status_code=404, detail='Space not found')
            )

            response = test_client.get(
                f'/api/v1/integration/spaces/{space_id}/sections',
                headers={'Authorization': 'Bearer test-token'},
            )

            assert response.status_code == 404
            assert response.json()['detail'] == 'Space not found'

    def test_get_space_sections_empty_sections(
        self, test_client, mock_space_detail_response
    ):
        """Test space sections retrieval with empty sections."""
        space_id = '1'

        with patch(
            'openhands.server.routes.integration.space.SpaceModule'
        ) as mock_space_module_class:
            mock_space_module = mock_space_module_class.return_value
            mock_space_module.get_space_detail = AsyncMock(
                return_value=mock_space_detail_response
            )
            # Mock response with no sections
            mock_space_module.get_list_sections = AsyncMock(return_value=None)

            response = test_client.get(
                f'/api/v1/integration/spaces/{space_id}/sections',
                headers={'Authorization': 'Bearer test-token'},
            )

            assert response.status_code == 200
            data = response.json()

            # Should return empty list when no sections
            assert data['data'] == []
            assert data['status'] == 'Get space sections success'

    def test_get_space_sections_unauthorized_on_space_check(self, test_client):
        """Test space sections retrieval when user unauthorized to access space."""
        space_id = '1'

        with patch(
            'openhands.server.routes.integration.space.SpaceModule'
        ) as mock_space_module_class:
            mock_space_module = mock_space_module_class.return_value
            mock_space_module.get_space_detail = AsyncMock(
                side_effect=HTTPException(status_code=401, detail='Unauthorized')
            )

            response = test_client.get(
                f'/api/v1/integration/spaces/{space_id}/sections',
                headers={'Authorization': 'Bearer unauthorized-token'},
            )

            assert response.status_code == 404
            assert response.json()['detail'] == 'Space not found'

    def test_get_space_sections_error_during_sections_fetch(
        self, test_client, mock_space_detail_response
    ):
        """Test space sections retrieval when error occurs during sections fetch."""
        space_id = '1'

        with patch(
            'openhands.server.routes.integration.space.SpaceModule'
        ) as mock_space_module_class:
            mock_space_module = mock_space_module_class.return_value
            mock_space_module.get_space_detail = AsyncMock(
                return_value=mock_space_detail_response
            )
            mock_space_module.get_list_sections = AsyncMock(
                side_effect=HTTPException(
                    status_code=500, detail='Internal server error'
                )
            )

            response = test_client.get(
                f'/api/v1/integration/spaces/{space_id}/sections',
                headers={'Authorization': 'Bearer test-token'},
            )

            assert response.status_code == 404
            assert response.json()['detail'] == 'Space not found'

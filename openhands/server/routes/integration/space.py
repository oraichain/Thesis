from typing import Optional

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel, Field

from openhands.server.modules.space import SpaceModule


# Response Models
class SpaceListItem(BaseModel):
    id: int = Field(description='Unique identifier for the space', example=123)
    title: str = Field(description='Space title or name', example='AI Research Project')
    description: str = Field(
        description="Description of the space's purpose",
        example='Collaborative research on AI algorithms',
    )
    created_at: str = Field(
        description='ISO timestamp when space was created',
        example='2024-01-10T09:00:00Z',
    )
    updated_at: str = Field(
        description='ISO timestamp of last space update', example='2024-01-15T10:30:00Z'
    )


class PaginationInfo(BaseModel):
    offset: Optional[int] = Field(
        description='Number of records skipped', example=0, default=None
    )
    limit: Optional[int] = Field(
        description='Maximum number of records returned', example=10, default=None
    )
    total: Optional[int] = Field(
        description='Total number of available records', example=25, default=None
    )
    has_more: Optional[bool] = Field(
        description='Whether more records are available', example=True, default=None
    )


class SpaceListResponse(BaseModel):
    data: list[SpaceListItem] = Field(
        description='List of spaces matching the request', default=[]
    )
    pagination: Optional[PaginationInfo] = Field(
        description='Pagination information for the results',
        strict=False,
        default=None,
    )
    status: str = Field(
        description='Response status message', example='Get list spaces success'
    )


class SpaceOwner(BaseModel):
    id: int = Field(description="Owner's unique identifier", example=789)
    name: str = Field(description="Owner's username", example='Test User')
    email: str = Field(description="Owner's email", example='test@example.com')


class SpaceDetail(BaseModel):
    id: int = Field(description='Unique space identifier', example=123)
    title: str = Field(description='Space title', example='AI Research Project')
    description: str = Field(
        description='Detailed description of the space',
        example='Collaborative research space focused on machine learning algorithms and their applications',
    )
    created_at: str = Field(
        description='ISO timestamp when space was created',
        example='2024-01-10T09:00:00Z',
    )
    updated_at: str = Field(
        description='ISO timestamp of last update', example='2024-01-15T10:30:00Z'
    )
    owner: SpaceOwner = Field(description='Space owner information')
    members_count: int = Field(description='Number of members in the space', example=5)


class SpaceDetailResponse(BaseModel):
    data: Optional[SpaceDetail] = Field(
        description='Complete space information', default=None
    )
    status: str = Field(
        description='Response status message', example='Get space detail success'
    )


class SpaceSection(BaseModel):
    id: int = Field(description='Unique section identifier', example=123)
    space_id: int = Field(description='Unique space identifier', example=123)
    title: str = Field(description='Section title', example='General Discussion')
    description: str = Field(
        description='Section description',
        example='Main area for general research discussions',
    )
    created_at: str = Field(
        description='ISO timestamp when section was created',
        example='2024-01-10T09:15:00Z',
    )


class SpaceSectionsResponse(BaseModel):
    data: Optional[list[SpaceSection]] = Field(
        description='List of sections in the space', default=[]
    )
    status: str = Field(
        description='Response status message', example='Get space sections success'
    )


class FastAPIErrorResponse(BaseModel):
    detail: str = Field(
        description='Error details from FastAPI', example='Unauthorized'
    )


space_router = APIRouter(
    prefix='/spaces',
    tags=['spaces'],
    responses={
        200: {'description': 'Spaces retrieved successfully'},
        401: {'description': 'Authentication required'},
        404: {'description': 'Resource not found'},
        500: {'description': 'Internal server error'},
    },
)


@space_router.get(
    '',
    summary='Get List of Spaces',
    description='Retrieves a paginated list of spaces accessible to the authenticated user. Supports filtering by title and pagination with offset/limit parameters.',
    response_description='Paginated list of spaces with metadata and pagination information',
    response_model=SpaceListResponse,
    responses={
        200: {
            'description': 'Spaces retrieved successfully',
            'model': SpaceListResponse,
        },
        401: {'description': 'Authentication required', 'model': FastAPIErrorResponse},
        500: {'description': 'Internal server error', 'model': FastAPIErrorResponse},
    },
)
async def get_list_space(
    request: Request, offset: int = 0, limit: int = 10, title: str | None = None
) -> dict | None:
    space_module = SpaceModule(request.headers.get('Authorization'))
    try:
        list_space, pagination = await space_module.get_list_space(offset, limit, title)
    except HTTPException:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail='Unauthorized',
        )

    return {
        'data': list_space or [],
        'pagination': pagination,
        'status': 'Get list spaces success',
    }


@space_router.get(
    '/{space_id}',
    summary='Get Space Details',
    description='Retrieves comprehensive details for a specific space including metadata, member information, and recent activity. Requires appropriate access permissions.',
    response_description='Complete space information with all details and metadata',
    response_model=SpaceDetailResponse,
    responses={
        200: {
            'description': 'Space details retrieved successfully',
            'model': SpaceDetailResponse,
        },
        401: {'description': 'Authentication required', 'model': FastAPIErrorResponse},
        404: {'description': 'Space not found', 'model': FastAPIErrorResponse},
        500: {'description': 'Internal server error', 'model': FastAPIErrorResponse},
    },
)
async def get_space_detail(
    request: Request,
    space_id: str,
) -> dict | None:
    space_module = SpaceModule(request.headers.get('Authorization'))
    try:
        space_detail = await space_module.get_space_detail(space_id)
    except HTTPException:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail='Space not found',
        )

    return {
        'data': space_detail,
        'status': 'Get space detail success',
    }


@space_router.get(
    '/{space_id}/sections',
    summary='Get Space Sections',
    description='Retrieves all sections within a specific space. Sections are organizational units within spaces that help categorize conversations and content. Requires valid space access permissions.',
    response_description='List of all sections in the specified space with their metadata',
    response_model=SpaceSectionsResponse,
    responses={
        200: {
            'description': 'Space sections retrieved successfully',
            'model': SpaceSectionsResponse,
        },
        401: {'description': 'Authentication required', 'model': FastAPIErrorResponse},
        404: {'description': 'Space not found', 'model': FastAPIErrorResponse},
        500: {'description': 'Internal server error', 'model': FastAPIErrorResponse},
    },
)
async def get_space_sections(
    request: Request,
    space_id: str,
) -> dict | None:
    space_module = SpaceModule(request.headers.get('Authorization'))
    try:
        # check space exist and user have permission to access this space
        await space_module.get_space_detail(space_id)
        space_sections = await space_module.get_list_sections(space_id)
    except HTTPException:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail='Space not found',
        )

    return {
        'data': space_sections,
        'status': 'Get space sections success',
    }

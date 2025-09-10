from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel, Field

from openhands.server.modules.space import SpaceModule


# Response Models
class SpaceListItem(BaseModel):
    space_id: str = Field(
        description='Unique identifier for the space', example='space_123'
    )
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
    member_count: int = Field(description='Number of members in the space', example=5)
    visibility: str = Field(description='Space visibility level', example='private')


class PaginationInfo(BaseModel):
    offset: int = Field(description='Number of records skipped', example=0)
    limit: int = Field(description='Maximum number of records returned', example=10)
    total: int = Field(description='Total number of available records', example=25)
    has_next: bool = Field(
        description='Whether more records are available', example=True
    )
    has_previous: bool = Field(
        description='Whether previous records exist', example=False
    )


class SpaceListResponse(BaseModel):
    data: list[SpaceListItem] = Field(
        description='List of spaces matching the request', default=[]
    )
    pagination: PaginationInfo = Field(
        description='Pagination information for the results'
    )
    status: str = Field(
        description='Response status message', example='Get list spaces success'
    )


class SpaceMember(BaseModel):
    user_id: str = Field(description='Unique user identifier', example='user_456')
    username: str = Field(description='Username of the member', example='ai_researcher')
    role: str = Field(description="Member's role in the space", example='contributor')
    joined_at: str = Field(
        description='ISO timestamp when user joined', example='2024-01-11T10:00:00Z'
    )


class SpaceOwner(BaseModel):
    user_id: str = Field(description="Owner's unique identifier", example='user_789')
    username: str = Field(description="Owner's username", example='research_lead')
    display_name: str = Field(description="Owner's display name", example='Dr. Smith')


class SpaceSettings(BaseModel):
    allow_guest_access: bool = Field(
        description='Whether guests can access the space', example=False
    )
    moderation_enabled: bool = Field(
        description='Whether content moderation is enabled', example=True
    )


class SpaceStats(BaseModel):
    total_conversations: int = Field(
        description='Total number of conversations in space', example=15
    )
    active_conversations: int = Field(
        description='Number of currently active conversations', example=3
    )
    total_messages: int = Field(
        description='Total number of messages across all conversations', example=450
    )


class SpaceDetail(BaseModel):
    space_id: str = Field(description='Unique space identifier', example='space_123')
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
    members: list[SpaceMember] = Field(description='List of space members', default=[])
    visibility: str = Field(description='Space visibility setting', example='private')
    settings: SpaceSettings = Field(description='Space configuration settings')
    stats: SpaceStats = Field(description='Space usage statistics')


class SpaceDetailResponse(BaseModel):
    data: SpaceDetail = Field(description='Complete space information')
    status: str = Field(
        description='Response status message', example='Get space detail success'
    )


class SpaceSection(BaseModel):
    section_id: str = Field(
        description='Unique section identifier', example='section_001'
    )
    title: str = Field(description='Section title', example='General Discussion')
    description: str = Field(
        description='Section description',
        example='Main area for general research discussions',
    )
    created_at: str = Field(
        description='ISO timestamp when section was created',
        example='2024-01-10T09:15:00Z',
    )
    updated_at: str = Field(
        description='ISO timestamp of last section activity',
        example='2024-01-15T11:30:00Z',
    )
    conversation_count: int = Field(
        description='Number of conversations in this section', example=8
    )
    last_activity: str = Field(
        description='ISO timestamp of last activity', example='2024-01-15T10:45:00Z'
    )
    order: int = Field(description='Display order of the section', example=1)
    is_public: bool = Field(
        description='Whether section is publicly accessible', example=True
    )


class SpaceSectionsResponse(BaseModel):
    data: list[SpaceSection] = Field(
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
        'data': list_space,
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

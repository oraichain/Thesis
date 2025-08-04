from enum import Enum
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, model_validator

from openhands.server.thesis_auth import (
    generate_access_token_from_api_key,
    generate_access_token_from_refresh_token,
)

auth_route = APIRouter(prefix='/auth')


class GrantType(Enum):
    API_KEY = 'api_key'
    REFRESH_TOKEN = 'refresh_token'


class GetAccessTokenRequest(BaseModel):
    grant_type: GrantType = GrantType.API_KEY
    api_key: Optional[str] = None
    refresh_token: Optional[str] = None

    @model_validator(mode='after')
    def validate_grant_type(self):
        grant_type = self.grant_type
        if not getattr(self, grant_type.value, None):
            raise ValueError(f'{grant_type.value} is required')
        return self


class GetAccessTokenResponse(BaseModel):
    message: str
    access_token: str
    refresh_token: str


@auth_route.post('/token')
async def get_token(request: GetAccessTokenRequest) -> GetAccessTokenResponse:
    if request.grant_type == GrantType.API_KEY:
        api_key = request.api_key
        response = await generate_access_token_from_api_key(api_key)
    else:
        refresh_token = request.refresh_token
        response = await generate_access_token_from_refresh_token(refresh_token)

    if not response.get('accessToken'):
        raise HTTPException(status_code=401, detail='Invalid API key or refresh token')
    return GetAccessTokenResponse(
        message='Get accesstoken successfully',
        access_token=response.get('accessToken'),
        refresh_token=response.get('refreshToken'),
    )

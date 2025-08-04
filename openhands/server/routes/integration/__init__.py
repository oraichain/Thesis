from fastapi import APIRouter

from openhands.server.routes.integration.auth import auth_route
from openhands.server.routes.integration.conversation import conversation_router

app = APIRouter(prefix='/api/v1/integration')

app.include_router(auth_route, tags=['auth'])
app.include_router(conversation_router, tags=['conversations'])

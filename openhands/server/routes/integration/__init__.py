from fastapi import APIRouter

from openhands.server.routes.integration.conversation import conversation_router

app = APIRouter(prefix='/api/v1/integration')

app.include_router(conversation_router)

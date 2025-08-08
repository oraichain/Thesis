from sqlalchemy import and_, delete
from openhands.core.logger import openhands_logger as logger
from openhands.server.models import SpaceSectionAction, SpaceSectionConfig
from openhands.server.db import database
from openhands.server.thesis_auth import (
    get_list_sections,
    get_list_space,
    get_space_detail,
    update_space_section_history,
)


class SpaceSectionModule:
    async def _get_space_section_config(self, space_id: int, space_section_id: int):
        """
        Get space section config by space_id and space_section_id
        """
        try:
            query = SpaceSectionConfig.select().where(
                and_(
                    SpaceSectionConfig.c.space_id == space_id,
                    SpaceSectionConfig.c.space_section_id == space_section_id,
                )
            )
            existing_record = await database.fetch_one(query)
            return existing_record
        except Exception as e:
            logger.error(f'Error getting space section config: {str(e)}')
            return None

    async def get_list_space(
        self, offset: int = 0, limit: int = 10, title: str | None = None
    ) -> tuple[list[dict] | None, dict | None]:
        list_space_response_data = await get_list_space(
            bearer_token=self.bearer_token, offset=offset, limit=limit, title=title
        )
        if not list_space_response_data:
            return [], {}
        if not list_space_response_data.get('data'):
            return [], {}
        return (
            list_space_response_data.get('data'),
            list_space_response_data.get('pagination'),
        )

    async def get_space_detail(self, space_id: str) -> dict | None:
        space_detail_response_data = await get_space_detail(
            bearer_token=self.bearer_token, space_id=space_id
        )
        if not space_detail_response_data:
            return None
        if not space_detail_response_data.get('data'):
            return None
        return space_detail_response_data.get('data')

    async def get_list_sections(self, space_id: str) -> list[dict] | None:
        list_section_response_data = await get_list_sections(
            bearer_token=self.bearer_token, space_id=space_id
        )
        if not list_section_response_data:
            return []
        if not list_section_response_data.get('data'):
            return []
        return list_section_response_data.get('data')

    async def update_space_section_history(
        self, space_id: str, section_id: str, conversation_id: str
    ):
        try:
            await update_space_section_history(
                space_id=space_id,
                section_id=section_id,
                conversation_id=conversation_id,
                bearer_token=self.bearer_token,
            )
        except Exception as e:
            logger.error(f'Error upserting space section config with raw SQL: {str(e)}')
            return False

    async def _update_space_section_config_hash(
        self, space_id: int, space_section_id: int, hash_config: str
    ):
        """
        Update hash_config for space section config
        """
        try:
            await database.execute(
                SpaceSectionConfig.update()
                .where(
                    and_(
                        SpaceSectionConfig.c.space_id == space_id,
                        SpaceSectionConfig.c.space_section_id == space_section_id,
                    )
                )
                .values(hash_config=hash_config)
            )
            return True
        except Exception as e:
            logger.error(f'Error updating space section config hash: {str(e)}')
            return False

    async def _delete_space_section_actions(self, space_id: int, space_section_id: int):
        """
        Delete all space section actions for a specific space_id and space_section_id
        """
        try:
            query = delete(SpaceSectionAction).where(
                and_(
                    SpaceSectionAction.c.space_id == space_id,
                    SpaceSectionAction.c.space_section_id == space_section_id,
                )
            )
            result = await database.execute(query)
            return result
        except Exception as e:
            logger.error(f'Error deleting space section actions: {str(e)}')
            return None


space_section_module = SpaceSectionModule()

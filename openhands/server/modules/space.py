from openhands.server.thesis_auth import (
    get_list_sections,
    get_list_space,
    get_space_detail,
)


class SpaceModule:
    def __init__(self, bearer_token: str):
        self.bearer_token = bearer_token

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

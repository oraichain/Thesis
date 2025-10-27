import os

import httpx
from dotenv import load_dotenv

from openhands.core.logger import openhands_logger as logger

load_dotenv()


class StrategyServerClient:
    def __init__(
        self,
        strategy_server_url: str = os.getenv('THESIS_STRATEGY_SERVER_URL')
        or 'http://localhost:9000',
    ):
        self.strategy_server_url = strategy_server_url

    async def get_matching_blueprint_id(
        self, space_id: int, space_section_id: int
    ) -> str | None:
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(
                    f'{self.strategy_server_url}/blueprints/by-space-section/{space_id}/{space_section_id}'
                )
                if response.status_code != 200:
                    return None
                return response.json()
        except Exception as e:
            logger.info(
                f'No matching blueprint id found for space {space_id} and section {space_section_id}: {e}'
            )
            return None

    async def create_strategy(self, blueprint_id: str) -> str | None:
        try:
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                strategy = await client.post(
                    f'{self.strategy_server_url}/strategy/',
                    json={'blueprint_id': blueprint_id},
                )
                if strategy.status_code != 200:
                    logger.error(
                        f'Error creating strategy: {strategy.status_code} - {strategy.text}'
                    )
                    return None
                strategy_data = strategy.json()
                if strategy_data:
                    return strategy_data['id']
                return None
        except Exception as e:
            logger.warning(f'Error creating strategy: {e}')
            return None

    async def execute_strategy(self, strategy_id: str, user_prompt: str) -> str | None:
        try:
            async with httpx.AsyncClient(
                timeout=120.0, follow_redirects=True
            ) as client:
                logger.info(
                    f'Executing strategy: {strategy_id} with user prompt: {user_prompt}'
                )
                response = await client.post(
                    f'{self.strategy_server_url}/strategy/{strategy_id}/execute/',
                    json={'user_prompt': user_prompt},
                )
                if response.status_code != 200:
                    logger.error(
                        f'Error executing strategy: {response.status_code} - {response.text}'
                    )
                    return None
                return str(response.json())
        except Exception as e:
            logger.warning(f'Error executing strategy: {e}')
            return None

    async def create_and_execute_strategy_background(
        self,
        blueprint_id: str,
        user_prompt: str,
        session_id: str,
        system_prompt: str | None,
    ) -> str | None:
        try:
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                strategy = await client.post(
                    f'{self.strategy_server_url}/strategy/create-and-execute/',
                    json={
                        'blueprint_id': blueprint_id,
                        'user_prompt': user_prompt,
                        'session_id': session_id,
                        'system_prompt': system_prompt if system_prompt else '',
                    },
                )
                if strategy.status_code != 200:
                    logger.error(
                        f'Error creating strategy: {strategy.status_code} - {strategy.text}'
                    )
                    return None
                strategy_data = strategy.json()
                if strategy_data:
                    return strategy_data['id']
                return None
        except Exception as e:
            logger.warning(f'Error creating strategy: {e}')
            return None

    async def get_strategy_final_output(self, strategy_id: str):
        """Poll for strategy result with retry logic for 400 status codes.

        Retries every 1s for up to 120s if the strategy is not yet finished (400).
        """
        try:
            async with httpx.AsyncClient(
                timeout=120.0, follow_redirects=True
            ) as client:
                response = await client.get(
                    f'{self.strategy_server_url}/strategy/{strategy_id}/wait-final-output/'
                )
                if response.status_code != 200:
                    logger.error(
                        f'Error getting strategy final output: {response.status_code} - {response.text}'
                    )
                    return None
                return str(response.json())
        except Exception as e:
            logger.warning(f'Error getting strategy final output: {e}')
            return None

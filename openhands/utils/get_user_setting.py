from openhands.core.config.llm_config import LLMConfig
from openhands.core.logger import openhands_logger as logger
from openhands.integrations.provider import (
    PROVIDER_TOKEN_TYPE,
)
from openhands.integrations.service_types import Repository
from openhands.server.settings import Settings
from openhands.server.shared import (
    SettingsStoreImpl,
    config,
)
from openhands.server.types import LLMAuthenticationError, MissingSettingsError


async def get_user_setting(user_id: str | None, useDefaultSettings: bool = True):
    settings_store = await SettingsStoreImpl.get_instance(config, user_id)
    settings = await settings_store.load()

    if not settings and useDefaultSettings:
        # # If no settings found for user, load default settings
        # default_store = await SettingsStoreImpl.get_instance(config, None)
        # settings = await default_store.load()
        # if not settings:
        settings = Settings.from_config()

    # use global config instead of user settings
    if settings:
        llm_config: LLMConfig = config.get_llm_config()

        settings.enable_default_condenser = config.enable_default_condenser
        settings.llm_model = llm_config.model
        settings.llm_api_key = llm_config.api_key
        settings.llm_base_url = llm_config.base_url

    return settings


async def settings_for_conversation(
    user_id: str | None,
    git_provider_tokens: PROVIDER_TOKEN_TYPE | None,
    selected_repository: Repository | None,
    selected_branch: str | None,
):
    settings = await get_user_setting(user_id)

    session_init_args: dict = {}
    if settings:
        session_init_args = {**settings.__dict__, **session_init_args}
        # We could use litellm.check_valid_key for a more accurate check,
        # but that would run a tiny inference.
        if (
            not settings.llm_api_key
            or settings.llm_api_key.get_secret_value().isspace()
        ):
            logger.warn(f'Missing api key for model {settings.llm_model}')
            raise LLMAuthenticationError(
                'Error authenticating with the LLM provider. Please check your API key'
            )
    else:
        logger.warn('Settings not present, not starting conversation')
        raise MissingSettingsError('Settings not found')

    session_init_args['git_provider_tokens'] = git_provider_tokens
    session_init_args['selected_repository'] = selected_repository
    session_init_args['selected_branch'] = selected_branch
    return session_init_args

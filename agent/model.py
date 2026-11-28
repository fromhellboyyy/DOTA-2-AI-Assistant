from __future__ import annotations

from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.openai import OpenAIProvider

from agent.config import Settings


def build_model(settings: Settings) -> OpenAIChatModel:
    return OpenAIChatModel(
        settings.model,
        provider=OpenAIProvider(base_url=settings.base_url, api_key=settings.api_key),
    )

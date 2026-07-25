from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from .config import (
    ContextMemoryConfig,
    _default_enable_forgetting,
    _normalize_tool_presets,
)

if TYPE_CHECKING:
    from .engine import ContextMemoryEngineV3


class ContextMemorySystem:
    _instance: ContextMemoryEngineV3 | None = None

    @classmethod
    def get_instance(
        cls,
        *,
        config: ContextMemoryConfig | dict[str, Any] | None = None,
        base_dir: str | Path | None = None,
        llm_preset: str = "",
        image_llm_preset: str = "",
        tool_presets: dict[str, str] | None = None,
        ask_timeout: float = 300.0,
        auto_resume_pending_jobs: bool = True,
        use_mock_llm: bool = False,
        enable_cleaning: bool = True,
        enable_forgetting: bool | None = None,
        init_config: bool = True,
    ) -> ContextMemoryEngineV3:
        from .engine import ContextMemoryEngineV3

        if cls._instance is None:
            if isinstance(config, ContextMemoryConfig):
                cfg_obj = config
            elif isinstance(config, dict):
                cfg_obj = ContextMemoryConfig.from_dict(config)
            else:
                effective_forgetting = (
                    _default_enable_forgetting()
                    if enable_forgetting is None
                    else bool(enable_forgetting)
                )
                cfg_obj = ContextMemoryConfig(
                    base_dir=base_dir,
                    llm_preset=llm_preset,
                    image_llm_preset=image_llm_preset,
                    tool_presets=_normalize_tool_presets(tool_presets),
                    ask_timeout=ask_timeout,
                    auto_resume_pending_jobs=auto_resume_pending_jobs,
                    use_mock_llm=use_mock_llm,
                    enable_cleaning=enable_cleaning,
                    enable_forgetting=effective_forgetting,
                    init_config=init_config,
                )
            cls._instance = ContextMemoryEngineV3(config=cfg_obj)
            return cls._instance

        if isinstance(config, ContextMemoryConfig):
            cls._instance.apply_config(config)
        elif isinstance(config, dict):
            cls._instance.apply_config(ContextMemoryConfig.from_dict(config))
        return cls._instance


def get_context_memory_engine(
    config: ContextMemoryConfig | dict[str, Any] | None = None,
    base_dir: str | Path | None = None,
    llm_preset: str = "",
    image_llm_preset: str = "",
    tool_presets: dict[str, str] | None = None,
    ask_timeout: float = 300.0,
    auto_resume_pending_jobs: bool = True,
    use_mock_llm: bool = False,
    enable_cleaning: bool = True,
    enable_forgetting: bool | None = None,
    init_config: bool = True,
):
    return ContextMemorySystem.get_instance(
        config=config,
        base_dir=base_dir,
        llm_preset=llm_preset,
        image_llm_preset=image_llm_preset,
        tool_presets=tool_presets,
        ask_timeout=ask_timeout,
        auto_resume_pending_jobs=auto_resume_pending_jobs,
        use_mock_llm=use_mock_llm,
        enable_cleaning=enable_cleaning,
        enable_forgetting=enable_forgetting,
        init_config=init_config,
    )

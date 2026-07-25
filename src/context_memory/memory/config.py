from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

def _default_enable_forgetting() -> bool:
    try:
        from context_memory.config import SETTING_CFG
    except Exception:
        return True
    memory_cfg = getattr(SETTING_CFG, "Memory", None)
    if memory_cfg is None:
        return True
    try:
        return bool(getattr(memory_cfg, "enable_forgetting", True))
    except Exception:
        return True


def _resolve_effective_max_context_window(llm_preset: str) -> int:
    try:
        from context_memory import config as _cfg_mod
    except Exception as exc:
        raise RuntimeError("failed to load config module while resolving llm max_context") from exc

    preset_name = str(llm_preset or "").strip() or "CONTEXT_MEMORY"
    user_cfg: dict[str, Any] = {}
    load_user_cfg = getattr(_cfg_mod, "_load_user_config", None)
    if callable(load_user_cfg):
        try:
            loaded = load_user_cfg()
            if isinstance(loaded, dict):
                user_cfg = loaded
        except Exception:
            user_cfg = {}

    raw_presets = user_cfg.get("llm_presets", {}) if isinstance(user_cfg, dict) else {}
    raw_target = raw_presets.get(preset_name) if isinstance(raw_presets, dict) else None
    if raw_target is None and preset_name != "CONTEXT_MEMORY" and isinstance(raw_presets, dict):
        raw_target = raw_presets.get("CONTEXT_MEMORY")
    if isinstance(raw_target, dict) and "max_context" not in raw_target:
        raise RuntimeError(
            f"llm preset <{preset_name}> missing key <max_context> in config file; program aborted"
        )

    try:
        llm_cfg = _cfg_mod.get_llm(preset_name)
    except Exception as exc:
        raise RuntimeError(f"llm preset not found or invalid: {preset_name}") from exc

    llm_max = getattr(llm_cfg, "max_context", None)
    if isinstance(llm_max, (int, float)) and llm_max > 0:
        return int(llm_max)
    raise RuntimeError(
        f"llm preset <{preset_name}> missing valid max_context; please set llm_presets.{preset_name}.max_context"
    )

TOOL_PRESET_KEYS: tuple[str, ...] = (
    "clean",
    "ingest",
    "query",
    "compress",
    "bucket_split",
    "text_chunk",
    "bucket_summary",
    "optimize",
    "image_extract",
)


def _normalize_tool_presets(tool_presets: dict[str, str] | None) -> dict[str, str]:
    if not isinstance(tool_presets, dict):
        return {}
    normalized: dict[str, str] = {}
    for k, v in tool_presets.items():
        key = str(k).strip().lower()
        val = str(v).strip()
        if key in TOOL_PRESET_KEYS and val:
            normalized[key] = val
    return normalized


@dataclass(slots=True)
class ContextMemoryConfig:
    """ContextMemory 引擎配置。

    Attributes:
        base_dir: 数据根目录；`None` 表示由外部稍后绑定。
        llm_preset: 主 LLM 预设名称。
        image_llm_preset: 图像理解 LLM 预设名称。
        tool_presets: 各工具链路对应的预设映射（如 query/compress 等）。
        ask_timeout: 单次 LLM 调用超时时间（秒）。
        auto_resume_pending_jobs: 启动后是否自动恢复未完成分片任务。
        use_mock_llm: 是否使用 mock LLM（用于离线测试）。
        enable_cleaning: 是否启用输入清洗。
        init_config: 初始化引擎时是否触发配置加载。
        evidence_versions: 证据文件保留版本数。
        auto_manage: 写入后是否自动执行压缩/分桶等治理。
        enable_forgetting: 是否启用遗忘机制（负权衰减与自动灰化）。
        max_bucket_depth: 桶树最大深度。
        max_memory_bytes: 进程内记忆缓存上限（字节）。
        auto_compress_trigger_ratio: 自动压缩触发阈值（上下文占用比例）。
        auto_split_trigger_ratio: 自动分桶触发阈值（上下文占用比例）。
        split_plan_target_items: 分桶规划目标条目数。
        split_plan_hard_cap: 分桶规划硬上限条目数。
        auto_split_cooldown_sec: 自动分桶冷却时间（秒）。
        auto_split_min_drop_abs: 自动分桶最小收益阈值。
        auto_split_max_round_per_manage: 单轮 auto manage 最多分桶次数。
        split_ingest_parallelism: 分片写入并发度。
        split_ingest_delay_min: 分片写入最小启动间隔（秒）。
        split_ingest_delay_max: 分片写入最大启动间隔（秒）。
        optimize_leaf_loss_threshold: optimize 叶子损耗阈值。
        gc_revision_retention_days: 历史 revision 保留天数。
        gc_gray_key_retention_days: 灰化 key 保留天数。
        gc_archived_bucket_retention_days: 归档桶保留天数。
        query_top_k_default: 查询默认 `top_k`。
        query_branch_expand_k: 递归扩展时默认展开桶数。
        query_branch_expand_bind_top_k: 是否将 `branch_expand_k` 绑定到 `top_k`。
        query_max_depth_default: 查询默认递归深度；`None` 表示使用引擎默认。
        query_mode_default: 默认查询模式（`auto`/`semantic`/`hybrid`）。
        global_recall_top_n: 全局召回阶段候选数 N。
        global_recall_top_m: 全局召回阶段参与重排桶数 M。
        global_recall_depth_limit: 全局召回时桶遍历深度上限。
        global_recall_time_budget_ms: 全局召回时间预算（毫秒）。
        global_recall_boost_weight: 全局召回得分融合权重。
    """
    base_dir: str | Path | None = None
    llm_preset: str = ""
    image_llm_preset: str = ""
    tool_presets: dict[str, str] = field(default_factory=dict)
    ask_timeout: float = 300.0
    auto_resume_pending_jobs: bool = True
    use_mock_llm: bool = False
    enable_cleaning: bool = True
    init_config: bool = True
    evidence_versions: int = 5
    auto_manage: bool = True
    enable_forgetting: bool = field(default_factory=_default_enable_forgetting)
    max_bucket_depth: int = 4
    max_memory_bytes: int = 1_000_000_000
    auto_compress_trigger_ratio: float = 0.70
    auto_split_trigger_ratio: float = 0.50
    split_plan_target_items: int = 180
    split_plan_hard_cap: int = 250
    auto_split_cooldown_sec: int = 600
    auto_split_min_drop_abs: float = 0.03
    auto_split_max_round_per_manage: int = 1
    split_ingest_parallelism: int = 16
    split_ingest_delay_min: float = 1.0
    split_ingest_delay_max: float = 3.0
    optimize_leaf_loss_threshold: float = 0.03
    gc_revision_retention_days: int = 14
    gc_gray_key_retention_days: int = 45
    gc_archived_bucket_retention_days: int = 45
    query_top_k_default: int = 5
    query_branch_expand_k: int = 5
    query_branch_expand_bind_top_k: bool = False
    query_max_depth_default: int | None = None
    query_mode_default: str = "auto"
    global_recall_top_n: int = 120
    global_recall_top_m: int = 8
    global_recall_depth_limit: int = 8
    global_recall_time_budget_ms: int = 80
    global_recall_boost_weight: float = 0.20

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ContextMemoryConfig":
        """从字典构建配置对象。

        Args:
            data: 配置字典；缺失字段将回落到默认值。

        Returns:
            ContextMemoryConfig: 规范化后的配置实例。
        """
        if not isinstance(data, dict):
            return cls()
        return cls(
            base_dir=data.get("base_dir"),
            llm_preset=str(data.get("llm_preset", "CONTEXT_MEMORY")),
            image_llm_preset=str(data.get("image_llm_preset", "KIMI2.6")),
            tool_presets=_normalize_tool_presets(data.get("tool_presets")),
            ask_timeout=float(data.get("ask_timeout", 180.0)),
            auto_resume_pending_jobs=bool(data.get("auto_resume_pending_jobs", True)),
            use_mock_llm=bool(data.get("use_mock_llm", False)),
            enable_cleaning=bool(data.get("enable_cleaning", True)),
            init_config=bool(data.get("init_config", True)),
            evidence_versions=int(data.get("evidence_versions", 5)),
            auto_manage=bool(data.get("auto_manage", True)),
            enable_forgetting=bool(data.get("enable_forgetting", _default_enable_forgetting())),
            max_bucket_depth=int(data.get("max_bucket_depth", 3)),
            max_memory_bytes=int(data.get("max_memory_bytes", 1_000_000_000)),
            auto_compress_trigger_ratio=float(data.get("auto_compress_trigger_ratio", 0.70)),
            auto_split_trigger_ratio=float(data.get("auto_split_trigger_ratio", 0.50)),
            split_plan_target_items=int(data.get("split_plan_target_items", 180)),
            split_plan_hard_cap=int(data.get("split_plan_hard_cap", 250)),
            auto_split_cooldown_sec=int(data.get("auto_split_cooldown_sec", 600)),
            auto_split_min_drop_abs=float(data.get("auto_split_min_drop_abs", 0.03)),
            auto_split_max_round_per_manage=int(data.get("auto_split_max_round_per_manage", 1)),
            split_ingest_parallelism=int(data.get("split_ingest_parallelism", 16)),
            split_ingest_delay_min=float(data.get("split_ingest_delay_min", 1.0)),
            split_ingest_delay_max=float(data.get("split_ingest_delay_max", 3.0)),
            optimize_leaf_loss_threshold=float(data.get("optimize_leaf_loss_threshold", 0.03)),
            gc_revision_retention_days=int(data.get("gc_revision_retention_days", 14)),
            gc_gray_key_retention_days=int(data.get("gc_gray_key_retention_days", 45)),
            gc_archived_bucket_retention_days=int(data.get("gc_archived_bucket_retention_days", 45)),
            query_top_k_default=int(data.get("query_top_k_default", 5)),
            query_branch_expand_k=int(data.get("query_branch_expand_k", 5)),
            query_branch_expand_bind_top_k=bool(data.get("query_branch_expand_bind_top_k", False)),
            query_max_depth_default=(
                int(data.get("query_max_depth_default"))
                if data.get("query_max_depth_default") is not None
                else None
            ),
            query_mode_default=str(data.get("query_mode_default", "auto")),
            global_recall_top_n=int(data.get("global_recall_top_n", 120)),
            global_recall_top_m=int(data.get("global_recall_top_m", 8)),
            global_recall_depth_limit=int(data.get("global_recall_depth_limit", 8)),
            global_recall_time_budget_ms=int(data.get("global_recall_time_budget_ms", 80)),
            global_recall_boost_weight=float(data.get("global_recall_boost_weight", 0.20)),
        )

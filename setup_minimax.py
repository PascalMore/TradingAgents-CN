#!/usr/bin/env python3
"""将 MiniMax 注册到 TradingAgents-CN 配置并设为默认"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.core.unified_config import UnifiedConfigManager
from app.models.config import LLMConfig

API_KEY = "sk-cp-6zYqByU7U26dpVHSh0Ys_cgJLZjrsyC4zLBet8QHThZRutXseCCKgx3MB9GTRP_eaUlLNCWwaQsjk0Z_8_UFkcv-uo0avsfR5JThPMCnzplyotV3x1-8MXY"

unified = UnifiedConfigManager()

print(f"当前默认模型: {unified.get_default_model()}")
print(f"当前 quick_analysis_model: {unified.get_quick_analysis_model()}")

# 添加 MiniMax LLMConfig
minimax_llm = LLMConfig(
    provider="minimax",
    model_name="MiniMax-Text-01",
    model_display_name="MiniMax Text 01 (旗舰级推理)",
    api_key=API_KEY,
    api_base="https://api.minimax.chat/v1",
    max_tokens=128000,
    temperature=0.5,
    timeout=180,
    retry_times=3,
    enabled=True,
    description="MiniMax 旗舰模型，支持128K上下文，推理能力强",
    capability_level=4,
    suitable_roles=["both"],
    features=["reasoning", "tool_calling", "long_context"],
    recommended_depths=["深度", "标准"],
    input_price_per_1k=0.01,
    output_price_per_1k=0.01,
    currency="CNY"
)

# 保存 MiniMax 配置
success = unified.save_llm_config(minimax_llm)
print(f"{'✅' if success else '❌'} MiniMax-Text-01 保存结果: {success}")

# 设为默认
unified.set_default_model("MiniMax-Text-01")
print("✅ 已设置 MiniMax-Text-01 为默认模型")

# 列出所有 LLM 配置
print("\n=== 当前 LLM 配置 ===")
for llm in unified.get_llm_configs():
    marker = " ◀" if f"{llm.provider}/{llm.model_name}" == unified.get_default_model() or llm.model_name == unified.get_default_model() else ""
    status = "✅" if llm.enabled else "❌"
    print(f"  {status} {llm.provider}/{llm.model_name}{marker}")

print(f"\n最终默认模型: {unified.get_default_model()}")
"""
指数数据模型 - 基于现有集合扩展
采用方案B: 在现有集合基础上扩展字段，保持向后兼容
"""
from datetime import datetime, date
from typing import Optional, Dict, Any, List, Literal
from pydantic import BaseModel, Field
from bson import ObjectId


class IndexBasicInfoExtended(BaseModel):
    """
    指数基础信息扩展模型 - 基于现有 index_basic_info 集合
    统一使用 symbol 作为主要指数代码字段
    """
    # === 标准化字段 (主要字段) ===
    symbol: str = Field(..., description="指数代码（不含后缀）")
    full_symbol: str = Field(..., description="完整指数化代码(含后缀)")
    name: str = Field(..., description="指数名称")
    fullname: str = Field(..., description="指数全称")

    # === 兼容字段 (保持向后兼容) ===
    code: Optional[str] = Field(None, description="指数代码，不含后缀(已废弃,使用symbol)")

    # === 基础信息字段 ===
    market: Optional[str] = Field(None, description="指数市场")
    publisher: Optional[str] = Field(None, description="指数发行商")
    category: Optional[str] = Field(None, description="指数分类")
    list_date: Optional[str] = Field(None, description="上市日期")
    base_date: Optional[str] = Field(None, description="指数基期")
    base_point: Optional[float] = Field(None, description="指数基点")
    desc: Optional[str] = Field(None, description="指数简介")
    source: Optional[str] = Field(None, description="数据来源")
    updated_at: Optional[datetime] = Field(None, description="更新时间")

    # 版本控制
    data_version: Optional[int] = Field(None, description="数据版本")
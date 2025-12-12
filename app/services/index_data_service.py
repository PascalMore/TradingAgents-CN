"""
指数数据服务层 - 统一数据访问接口
基于现有MongoDB集合，提供标准化的数据访问服务
"""
import logging
from datetime import datetime, date
from typing import Optional, Dict, Any, List
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.core.database import get_mongo_db
from app.models.index_models import (
    IndexBasicInfoExtended
)


logger = logging.getLogger(__name__)


class IndexDataService:
    """
    指数数据服务 - 统一数据访问层
    基于现有集合扩展，保持向后兼容
    """
    
    def __init__(self):
        self.basic_info_collection = "index_basic_info"
        #self.market_quotes_collection = "market_quotes"
    
    async def get_index_basic_info(
        self,
        symbol: str,
        source: Optional[str] = None
    ) -> Optional[IndexBasicInfoExtended]:
        """
        获取指数基础信息
        Args:
            symbol: 指数代码（指数代码不一定是6位）
            source: 数据源 (tushare/akshare/baostock/multi_source)，默认优先级：tushare > multi_source > akshare > baostock
        Returns:
            IndexBasicInfoExtended: 扩展的指数基础信息
        """
        try:
            db = get_mongo_db()
            # 对于不足6位的代码，左侧用0补齐，如果大于等于6位，则不做如何处理
            symbol6 = str(symbol).zfill(6)

            # 🔥 构建查询条件
            query = {"$or": [{"symbol": symbol6}, {"code": symbol6}]}

            if source:
                # 指定数据源
                query["source"] = source
                doc = await db[self.basic_info_collection].find_one(query, {"_id": 0})
            else:
                # 🔥 未指定数据源，按优先级查询
                source_priority = ["tushare", "multi_source", "akshare", "baostock"]
                doc = None

                for src in source_priority:
                    query_with_source = query.copy()
                    query_with_source["source"] = src
                    doc = await db[self.basic_info_collection].find_one(query_with_source, {"_id": 0})
                    if doc:
                        logger.debug(f"✅ 使用数据源: {src}")
                        break

                # 如果所有数据源都没有，尝试不带 source 条件查询（兼容旧数据）
                if not doc:
                    doc = await db[self.basic_info_collection].find_one(
                        {"$or": [{"symbol": symbol6}, {"code": symbol6}]},
                        {"_id": 0}
                    )
                    if doc:
                        logger.warning(f"⚠️ 使用旧数据（无 source 字段）: {symbol6}")

            if not doc:
                return None

            # 数据标准化处理
            standardized_doc = self._standardize_basic_info(doc)

            return IndexBasicInfoExtended(**standardized_doc)

        except Exception as e:
            logger.error(f"获取指数基础信息失败 symbol={symbol}, source={source}: {e}")
            return None
        
    async def update_index_basic_info(
        self,
        symbol: str,
        update_data: Dict[str, Any],
        source: str = "tushare"
    ) -> bool:
        """
        更新指数基础信息
        Args:
            symbol: 指数代码
            update_data: 更新数据
            source: 数据源 (tushare/akshare/baostock)，默认 tushare
        Returns:
            bool: 更新是否成功
        """
        try:
            db = get_mongo_db()
            symbol6 = str(symbol).zfill(6)

            # 添加更新时间
            update_data["updated_at"] = datetime.utcnow()

            # 确保symbol字段存在
            if "symbol" not in update_data:
                update_data["symbol"] = symbol6

            # 🔥 确保 code 字段存在
            if "code" not in update_data:
                update_data["code"] = symbol6

            # 🔥 确保 source 字段存在
            if "source" not in update_data:
                update_data["source"] = source

            # 🔥 执行更新 (使用 code + source 联合查询)
            result = await db[self.basic_info_collection].update_one(
                {"code": symbol6, "source": source},
                {"$set": update_data},
                upsert=True
            )

            return result.modified_count > 0 or result.upserted_id is not None

        except Exception as e:
            logger.error(f"更新指数信息失败 symbol={symbol}, source={source}: {e}")
            return False
        
    def _standardize_basic_info(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """
        标准化指数基础信息数据
        将现有字段映射到标准化字段
        """
        # 保持现有字段不变
        result = doc.copy()

        # 获取指数代码 (优先使用symbol，兼容code)
        symbol = doc.get("symbol") or doc.get("code", "")
        result["symbol"] = symbol

        # 兼容旧字段
        if "code" in doc and "symbol" not in doc:
            result["code"] = doc["code"]
        
        result["full_symbol"] = doc["full_symbol"]

        # 字段映射和标准化
        result["name"] = doc.get("name")  # 指数名字标准化
        result["fullname"] = doc.get("fullname")  # 指数全称标准化
        result["market"] = doc.get("market")  # 指数市场标准化
        result["publisher"] = doc.get("publisher")   # 指数发行商标准化
        result["category"] = doc.get("category")  # 指数分类标准化
        result["base_date"] = doc.get("base_date")  # 指数基期标准化
        result["base_point"] = doc.get("base_point")   # 指数基点标准化
        result["desc"] = doc.get("desc") # 指数介绍标准化
        result["data_version"] = 1

        # 处理日期字段格式转换
        list_date = doc.get("list_date")
        if list_date and isinstance(list_date, int):
            # 将整数日期转换为字符串格式 (YYYYMMDD -> YYYY-MM-DD)
            date_str = str(list_date)
            if len(date_str) == 8:
                result["list_date"] = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            else:
                result["list_date"] = str(list_date)
        elif list_date:
            result["list_date"] = str(list_date)

        return result
        
# 全局服务实例
_index_data_service = None

def get_index_data_service() -> IndexDataService:
    """获取股票数据服务实例"""
    global _index_data_service
    if _index_data_service is None:
        _index_data_service = IndexDataService()
    return _index_data_service

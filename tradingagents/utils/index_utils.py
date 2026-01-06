"""
指数工具函数
提供指数代码识别、分类和处理功能
"""

import re
from typing import Dict, Tuple, Optional
from enum import Enum

# 导入统一日志系统
from tradingagents.utils.logging_init import get_logger
logger = get_logger("agents")

class IndexMarket(Enum):
    """指数市场枚举"""
    CHINA_A = "china_a"      # 中国A股
    HONG_KONG = "hong_kong"  # 港股
    US = "us"                # 美股
    EU = "eu"                # 欧洲    
    UNKNOWN = "unknown"      # 未知

class IndexUtils:
    """指数工具类"""
    @staticmethod
    def identify_index_market(ticker: str) -> IndexMarket:
        """
        识别指数代码所属市场

        Args:
            ticker: 指数代码

        Returns:
            IndexMarket: 指数所属市场
        """
        if not ticker:
            return IndexMarket.UNKNOWN

        ticker = str(ticker).strip().upper()
        # 中国市场
        if re.match(r'^\w+\.(?:SH|SZ)$', ticker):
            return IndexMarket.CHINA_A
        
        # TODO：根据其他信息区分市场
        
        return IndexMarket.UNKNOWN
    
    @staticmethod
    def get_currency_info(ticker: str) -> Tuple[str, str]:
        """
        根据指数代码获取货币信息
        
        Args:
            mticker: 指数代码
            
        Returns:
            Tuple[str, str]: (货币名称, 货币符号)
        """
        market = IndexUtils.identify_index_market(ticker)

        if market == IndexMarket.CHINA_A:
            return "人民币", "¥"
        elif market == IndexMarket.HONG_KONG:
            return "港币", "HK$"
        elif market == IndexMarket.US:
            return "美元", "$"
        else:
            return "未知", "?"
    
    @staticmethod
    def get_data_source(ticker: str) -> str:
        """
        根据指数代码获取推荐的数据源
        
        Args:
            ticker: 指数代码
            
        Returns:
            str: 数据源名称
        """
        market = IndexUtils.identify_index_market(ticker)
        
        if market == IndexMarket.CHINA_A:
            return "china_unified"  # 使用统一的中国指数数据源
        elif market == IndexMarket.HONG_KONG:
            return "unknown"  # TODO: 香港指数源还未确定
        elif market == IndexMarket.US:
            return "unknown"   # TODO: 美股指数源还未确定
        else:
            return "unknown"
        
    @staticmethod
    def get_market_info(ticker: str) -> Dict:
        """
        获取指数市场的详细信息
        
        Args:
            ticker: 指数代码
            
        Returns:
            Dict: 市场信息字典
        """
        market = IndexUtils.identify_index_market(ticker)
        print(market)
        currency_name, currency_symbol = IndexUtils.get_currency_info(ticker)
        data_source = IndexUtils.get_data_source(ticker)
        
        market_names = {
            IndexMarket.CHINA_A: "中国A股",
            IndexMarket.HONG_KONG: "港股",
            IndexMarket.US: "美股",
            IndexMarket.UNKNOWN: "未知市场"
        }
        
        return {
            "ticker": ticker,
            "market": market.value,
            "market_name": market_names[market],
            "currency_name": currency_name,
            "currency_symbol": currency_symbol,
            "data_source": data_source,
            "is_china": market == IndexMarket.CHINA_A,
            "is_hk": market == IndexMarket.HONG_KONG,
            "is_us": market == IndexMarket.US
        }

import asyncio
import logging
import sys
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta, timezone
import tushare as ts

from pathlib import Path
# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
from app.core.database import init_database, get_mongo_db
from app.core.rate_limiter import get_tushare_rate_limiter

from tradingagents.dataflows.providers.china.tushare import TushareProvider


logger = logging.getLogger(__name__)
ts.set_token("3b633dea5db1ed3a161359154cb19b1d71c786370643d4519f302e6b")
api = ts.pro_api()
    
async def sync_financial_data(symbols: List[str] = None, start_date: str = "20000101", end_date: str = "20251116") -> Dict[str, Any]:
    """
    同步财务数据

    Args:
        symbols: 股票代码列表，None表示同步所有股票
        limit: 获取财报期数，默认20期（约5年数据）
        job_id: 任务ID（用于进度跟踪）
    """ 
    provider = TushareProvider()
    rate_limiter =  get_tushare_rate_limiter(tier="basic", safety_margin=0.8)
    """初始化同步服务"""
    success = await provider.connect()
    if not success:
        raise RuntimeError("❌ Tushare连接失败，无法启动同步服务")

    logger.info(f"🔄 开始同步财务数据 (获取时间：{start_date}-{end_date})...")

    stats = {
        "total_processed": 0,
        "success_count": 0,
        "error_count": 0,
        "start_time": datetime.utcnow(),
        "errors": []
    }
    try:
    # 获取股票列表
        # 初始化数据库连接
        print("🔌 正在连接数据库...")
        await init_database()
        print("✅ 数据库连接成功")

        db = get_mongo_db()
        if symbols is None:
            cursor = db.stock_basic_info.find(
                {
                    "$or": [
                        {"market_info.market": "CN"},  # 新数据结构
                        {"category": "stock_cn"},      # 旧数据结构
                        {"market": {"$in": ["主板", "创业板", "科创板", "北交所"]}}  # 按市场类型
                    ]
                },
                {"code": 1}
            )
            symbols = [doc["code"] async for doc in cursor]
            logger.info(f"📋 从 stock_basic_info 获取到 {len(symbols)} 只股票")

        stats["total_processed"] = len(symbols)
        logger.info(f"📊 需要同步 {len(symbols)} 只股票财务数据")

        # 批量处理
        for i, symbol in enumerate(symbols):
            try:
                # 速率限制
                await rate_limiter.acquire()

                # 获取财务数据（指定获取期数）
                #financial_data = await provider.get_financial_data_all(symbol, start_date, end_date, report_type=1)
                financial_data = await provider.get_financial_data(symbol, limit=20)
                #print(financial_data)

                if financial_data:
                    # 保存财务数据
                    success = True
                    if success:
                        stats["success_count"] += 1
                    else:
                        stats["error_count"] += 1
                else:
                    logger.warning(f"⚠️ {symbol}: 无财务数据")
                
                # 进度日志和进度跟踪
                if (i + 1) % 20 == 0:
                    progress = int((i + 1) / len(symbols) * 100)
                    logger.info(f"📈 财务数据同步进度: {i + 1}/{len(symbols)} ({progress}%) "
                               f"(成功: {stats['success_count']}, 错误: {stats['error_count']})")
                    # 输出速率限制器统计
                    limiter_stats = rate_limiter.get_stats()
                    logger.info(f"   速率限制: {limiter_stats['current_calls']}/{limiter_stats['max_calls']}次")
            
            except Exception as e:
                stats["error_count"] += 1
                stats["errors"].append({
                    "code": symbol,
                    "error": str(e),
                    "context": "sync_financial_data"
                })
                logger.error(f"❌ {symbol} 财务数据同步失败: {e}")

        # 完成统计
        stats["end_time"] = datetime.utcnow()
        stats["duration"] = (stats["end_time"] - stats["start_time"]).total_seconds()

        logger.info(f"✅ 财务数据同步完成: "
                   f"成功 {stats['success_count']}/{stats['total_processed']}, "
                   f"错误 {stats['error_count']} 个, "
                   f"耗时 {stats['duration']:.2f} 秒")

    except Exception as e:
            logger.error(f"❌ 财务数据同步失败: {e}")
            stats["errors"].append({"error": str(e), "context": "sync_financial_data"})
            return stats
    
async def main():
    try:
        res = await sync_financial_data(["000001.SZ", "000002.SZ"], start_date="20240101", end_date="20251110")
        print(res)

    except Exception as e:
        logger.error(f"❌ Tushare财务数据同步失败: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())



    #2025/11/17 增加获取股票公告区间财务数据的接口
    async def get_financial_data_all(self, symbol: str, start_date: str = None, end_date: str = None, report_type: int = 1) -> Optional[Dict[str, Any]]:
        """
        获取区间所有报告期财务数据

        Args:
            symbol: 股票代码
            report_type: 报告类型 (默认是l表示合并报表)
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            财务数据字典，包含利润表、资产负债表、现金流量表和财务指标
        """
        if not self.is_available():
            return None
        
        try:
            ts_code = self._normalize_ts_code(symbol)
            self.logger.debug(f"📊 获取Tushare财务数据: {ts_code}, 类型: {report_type}")

            # 构建查询参数
            query_params = {
                'ts_code': ts_code,
                'report_type': report_type,
                'start_date': start_date,
                'end_date': end_date
            }

            financial_data = {}

            # 1. 获取利润表数据 (income statement)
            try:
                income_df = await asyncio.to_thread(
                    self.api.income,
                    **query_params
                )
                print(income_df)
                if income_df is not None and not income_df.empty:
                    financial_data['income_statement'] = income_df.to_dict('records')
                    self.logger.debug(f"✅ {ts_code} 利润表数据获取成功: {len(income_df)} 条记录")
                else:
                    self.logger.debug(f"⚠️ {ts_code} 利润表数据为空")
            except Exception as e:
                self.logger.warning(f"❌ 获取{ts_code}利润表数据失败: {e}")

            # 2. 获取资产负债表数据 (balance sheet)
            try:
                balance_df = await asyncio.to_thread(
                    self.api.balancesheet,
                    **query_params
                )
                if balance_df is not None and not balance_df.empty:
                    financial_data['balance_sheet'] = balance_df.to_dict('records')
                    self.logger.debug(f"✅ {ts_code} 资产负债表数据获取成功: {len(balance_df)} 条记录")
                else:
                    self.logger.debug(f"⚠️ {ts_code} 资产负债表数据为空")
            except Exception as e:
                self.logger.warning(f"❌ 获取{ts_code}资产负债表数据失败: {e}")

            # 3. 获取现金流量表数据 (cash flow statement)
            try:
                cashflow_df = await asyncio.to_thread(
                    self.api.cashflow,
                    **query_params
                )
                if cashflow_df is not None and not cashflow_df.empty:
                    financial_data['cashflow_statement'] = cashflow_df.to_dict('records')
                    self.logger.debug(f"✅ {ts_code} 现金流量表数据获取成功: {len(cashflow_df)} 条记录")
                else:
                    self.logger.debug(f"⚠️ {ts_code} 现金流量表数据为空")
            except Exception as e:
                self.logger.warning(f"❌ 获取{ts_code}现金流量表数据失败: {e}")

            # 4. 获取财务指标数据 (financial indicators)
            try:
                indicator_df = await asyncio.to_thread(
                    self.api.fina_indicator,
                    **query_params
                )
                if indicator_df is not None and not indicator_df.empty:
                    financial_data['financial_indicators'] = indicator_df.to_dict('records')
                    self.logger.debug(f"✅ {ts_code} 财务指标数据获取成功: {len(indicator_df)} 条记录")
                else:
                    self.logger.debug(f"⚠️ {ts_code} 财务指标数据为空")
            except Exception as e:
                self.logger.warning(f"❌ 获取{ts_code}财务指标数据失败: {e}")

            # 5. 获取主营业务构成数据 (可选)
            try:
                mainbz_df = await asyncio.to_thread(
                    self.api.fina_mainbz,
                    **query_params
                )
                if mainbz_df is not None and not mainbz_df.empty:
                    financial_data['main_business'] = mainbz_df.to_dict('records')
                    self.logger.debug(f"✅ {ts_code} 主营业务构成数据获取成功: {len(mainbz_df)} 条记录")
                else:
                    self.logger.debug(f"⚠️ {ts_code} 主营业务构成数据为空")
            except Exception as e:
                self.logger.debug(f"获取{ts_code}主营业务构成数据失败: {e}")  # 主营业务数据不是必需的，保持debug级别

            if financial_data:
                # 标准化财务数据
                standardized_data = self._standardize_tushare_financial_data(financial_data, ts_code)
                self.logger.info(f"✅ {ts_code} Tushare财务数据获取完成: {len(financial_data)} 个数据集")
                return standardized_data
            else:
                self.logger.warning(f"⚠️ {ts_code} 未获取到任何Tushare财务数据")
                return None

        except Exception as e:
            self.logger.error(f"❌ 获取Tushare财务数据失败 symbol={symbol}: {e}")
            return None
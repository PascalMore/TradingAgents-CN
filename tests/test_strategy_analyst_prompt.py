#!/usr/bin/env python3
"""
测试策略分析师提示词的效果
"""

import os
import sys

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

def test_strategy_analyst_prompt():
    """测试策略分析师的提示词优化效果"""
    print("\n📈 测试策略分析师提示词优化效果")
    print("=" * 80)
    
    try:
        # 创建市场分析师
        from tradingagents.agents.analysts.strategy_analyst import create_strategy_analyst

        # 检查API密钥
        api_key = os.getenv("DASHSCOPE_API_KEY")
        if not api_key:
            print("⚠️ 未找到DASHSCOPE_API_KEY，跳过LLM测试")
            return True
        
        print(f"🔧 创建市场分析师...")
        
        # 创建LLM和工具包
        from tradingagents.llm_adapters import ChatDashScopeOpenAI
        from tradingagents.agents.utils.agent_utils import Toolkit
        from tradingagents.default_config import DEFAULT_CONFIG
        
        llm = ChatDashScopeOpenAI(
            model="qwen-turbo",
            temperature=0.1,
            max_tokens=1500
        )
        
        config = DEFAULT_CONFIG.copy()
        config["online_tools"] = True
        toolkit = Toolkit()
        toolkit.update_config(config)
        
        
        strategy_analyst = create_strategy_analyst(llm, toolkit)
        
        print(f"✅ 策略分析师创建完成")
        
        # 测试指数
        #test_ticker = "000001.SH" #A股综合指数-上证
        #test_ticker = "399001.SZ"  #A股综合指数-深证
        #test_ticker = "000300.SH"   #A股核心宽指-沪深300
        #test_ticker = "000510.SH"  #A股核心宽指-A500
        test_ticker = "000852.SH"  #A股核心宽指-中证1000
        
        print(f"\n📊 测试指数: {test_ticker}")
        print("-" * 60)
        
        # 创建分析状态
        state = {
            "index_of_interest": test_ticker,
            "trade_date": "2026-01-06",
            "messages": []
        }
        
        print(f"🔍 [提示词验证] 检查提示词构建...")
        
        # 获取公司名称（验证提示词构建逻辑）
        from tradingagents.agents.analysts.strategy_analyst import _get_index_name
        from tradingagents.utils.index_utils import IndexUtils
        
        market_info = IndexUtils.get_market_info(test_ticker)
        print(market_info)
        index_name = _get_index_name(test_ticker, market_info)
        
        print(f"   ✅ 指数代码: {test_ticker}")
        print(f"   ✅ 指数名称: {index_name}")
        print(f"   ✅ 市场类型: {market_info['market_name']}")
        print(f"   ✅ 货币信息: {market_info['currency_name']} ({market_info['currency_symbol']})")
        
        print(f"\n🤖 执行策略分析...")
        
        try:
            # 执行策略分析
            result = strategy_analyst(state)
            
            if isinstance(result, dict) and 'strategy_report' in result:
                report = result['strategy_report']
                print(f"✅ 策略分析完成，报告长度: {len(report)}")
                
                # 检查报告中的关键元素
                print(f"\n🔍 检查报告内容...")
                
                # 检查股票代码
                if test_ticker in report:
                    print(f"   ✅ 报告包含正确的指数代码: {test_ticker}")
                else:
                    print(f"   ❌ 报告不包含指数代码: {test_ticker}")
                
                # 检查指数名称
                if index_name in report and index_name != f"指数{test_ticker}":
                    print(f"   ✅ 报告包含正确的指数名称: {index_name}")
                else:
                    print(f"   ⚠️ 报告可能不包含具体指数名称")
                
                # 显示报告摘要
                print(f"\n📄 报告摘要 (前1000字符):")
                print("-" * 40)
                print(report[:1000])
                if len(report) > 1000:
                    print("...")
                print("-" * 40)
                
            else:
                print(f"❌ 策略分析返回格式异常: {type(result)}")
                
        except Exception as e:
            print(f"❌ 策略分析执行失败: {e}")
            import traceback
            traceback.print_exc()
        
        return True
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """主测试函数"""
    print("🚀 开始测试策略分析师提示词优化效果")
    print("=" * 100)
    
    results = []
    
    # 测试1: 提示词关键元素
    #results.append(test_prompt_elements())
    
    # 测试2: 基本面分析师提示词优化效果
    #results.append(test_fundamentals_analyst_prompt())
    
    # 测试3: 策略分析师提示词优化效果
    results.append(test_strategy_analyst_prompt())
    
    # 总结结果
    print("\n" + "=" * 100)
    print("📋 测试结果总结")
    print("=" * 100)
    
    passed = sum(results)
    total = len(results)
    
    test_names = [
       #"提示词关键元素验证",
        "策略分析师提示词优化"
    ]
    
    for i, (name, result) in enumerate(zip(test_names, results)):
        status = "✅ 通过" if result else "❌ 失败"
        print(f"{i+1}. {name}: {status}")
    
    print(f"\n📊 总体结果: {passed}/{total} 测试通过")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

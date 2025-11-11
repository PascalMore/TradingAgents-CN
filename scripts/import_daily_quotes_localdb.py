import sys
import os
import time
import pandas as pd
from pymongo import MongoClient

# MongoDB 连接配置
# Docker 内部运行时使用服务名 "mongodb"
# 宿主机运行时使用 "localhost"
MONGO_URI_DOCKER = "mongodb://myq:6812345@mongodb:27017/"
MONGO_URI_HOST = "mongodb://myq:6812345@localhost:27017/"
SRC_DB_NAME = "quantaxis"
DES_DB_NAME = "tradingagents"
LOCAL_FILE_PATH = "E:\\Data\\历史行情"

def connect_mongodb(use_docker: bool = True) -> MongoClient:
    """连接到 MongoDB

    Args:
        use_docker: True=在 Docker 容器内运行（使用 mongodb 服务名）
                   False=在宿主机运行（使用 localhost）
    """
    mongo_uri = MONGO_URI_DOCKER if use_docker else MONGO_URI_HOST
    env_name = "Docker 容器内" if use_docker else "宿主机"

    print(f"\n🔌 连接到 MongoDB ({env_name})...")
    print(f"   URI: {mongo_uri.replace('tradingagents123', '***')}")

    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        # 测试连接
        client.admin.command('ping')
        print(f"✅ MongoDB 连接成功")
        return client

    except Exception as e:
        print(f"❌ 错误: MongoDB 连接失败: {e}")
        if use_docker:
            print(f"   请确保在 Docker 容器内运行，或使用 --host 参数在宿主机运行")
            print(f"   检查容器: docker ps | grep mongodb")
        else:
            print(f"   请确保 MongoDB 容器正在运行并映射到宿主机端口 27017")
            print(f"   检查端口映射: docker ps | grep 27017")
        sys.exit(1)

def get_file_data(prefix: str):
    file_names = [item for item in os.listdir(LOCAL_FILE_PATH) if item.startswith(prefix)]
    tol_df = pd.DataFrame()
    for f in file_names:
        df = pd.read_csv(os.path.join(LOCAL_FILE_PATH, f), encoding="GBK")
        df = df.loc[:,~df.columns.str.contains("Unnamed")]
        df.drop(["简称"],axis=1,inplace=True)
        tol_df = pd.concat([tol_df, df])
    
    tol_df.rename(columns=
    {"代码":"full_symbol", "日期":"trade_date", "前收盘价(元)":"pre_close", "涨跌(元)":"change",  "涨跌幅(%)":"pct_chg",
    "开盘价(元)":"open","最高价(元)":"high","最低价(元)":"low","收盘价(元)":"close","成交量(股)":"volume","成交金额(元)":"amount"
    }, inplace=True)
    #增加一些附加信息
    tol_df["version"]=1
    tol_df["market"]="CN"
    tol_df["period"]="daily"
    tol_df["data_source"]="tushare"
    tol_df["code"] = tol_df["full_symbol"].map(lambda x: x.split(".")[0])
    tol_df["symbol"] = tol_df["full_symbol"].map(lambda x: x.split(".")[0])

    return tol_df   

def insert_data(target_doc, rd):
    try:
        ins_res=target_doc.insert_many(rd.to_dict(orient='records'))
        print("Bulk insert result: {}条记录".format(len(ins_res.inserted_ids)))
        return 0
    except Exception as err:
        print(err)
        return 1

def main():
    """主函数"""
    client = connect_mongodb(use_docker=False)
    col_src = client[SRC_DB_NAME]
    col_des = client[DES_DB_NAME]
    sat_tot_cnt = 0
    sat_suc = 0
    sat_fail=""
    #excels = ["1", "2", "3", "4", "5", "6", "7"]
    excels = ["8"]
    for i in excels:
        print("Insert Part:"+i)
        part_df = get_file_data(i)
        target_doc = col_src.get_collection("stock_day")
        sat_tot_cnt = part_df["code"].unique().size
        sat_suc=0
        for c in part_df["code"].unique():
            #print(part_df[part_df["code"]==c]["trade_date"].min())
            src_df=pd.DataFrame(list(target_doc.find({"code":c, "date":{"gte": part_df[part_df["code"]==c]["trade_date"].min()}})))
            if len(src_df)>0:
                src_df.drop(["_id","date_stamp"],axis=1, inplace=True)
                src_df.rename(columns={"vol":"volume","date":"trade_date"},inplace=True)
                extra_df = part_df[part_df["code"]==c]
                des_df=pd.merge(extra_df, src_df, how="left", on=["code","trade_date"])
            else:
                des_df=part_df[part_df["code"]==c]
            des_df["created_at"]=time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
            des_df["updated_at"]=time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
            print(des_df)
            if insert_data(col_des.get_collection("stock_daily_quotes"), des_df)>0:
                sat_fail += "," +c
            else:
                sat_suc += 1

            print("进度：{}/{}, 失败：{}".format(sat_suc,sat_tot_cnt,sat_fail))

    return 0


if __name__ == "__main__":
    main()

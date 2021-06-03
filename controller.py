# -*- coding: utf-8 -*-
import index
import requests
import pandas as pd

import logging
logger = logging.getLogger("controller")
logger.setLevel(logging.DEBUG)

from api_request import api_http

def apiBasicInfo():
    info = {
        "http_api": "http://api.tushare.pro",
        "token": "302c96ee4c114c2438ff3014d36e0b783ee331e07ce70dfc417a992e",
        "protocol": "http"
    }
    return info


def getConnection():
    return index.init_db_con

def get_hist_data_by_symbol(symbol):
    print(symbol)

    try:
        conn = getConnection()

        ts_code = transfer_from_symbol(symbol)
        print(ts_code)
        hist_data_sql = "SELECT trade_date, open, close, low, high FROM stockshistoricaldatas WHERE ts_code=%s"
        hist_data_df = pd.read_sql(hist_data_sql, con=conn, params=[ts_code])
        print(hist_data_df)

        # symbol_number = get_number_from_symbol(symbol)
        stock_info_sql = "SELECT * FROM stocks WHERE symbol=%s"
        stock_info_df = pd.read_sql(stock_info_sql, con=conn, params=[symbol])

        result = get_data_list_from_df(stock_info_df, hist_data_df)

        return result
    
    except Exception as e:
        logger.error(e)
        logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")
        raise e

def get_hist_data_page(request_body):
    """
    分页获取历史数据信息
    """
    try:
        conn = getConnection()

        ts_code = transfer_from_symbol(request_body.get("symbol"))
        print(ts_code)
        hist_data_sql = "SELECT trade_date, open, close, low, high FROM stockshistoricaldatas WHERE ts_code=%s"
        hist_data_df = pd.read_sql(hist_data_sql, con=conn, params=[ts_code])

        # symbol_number = get_number_from_symbol(symbol)
        stock_info_sql = "SELECT * FROM stocks WHERE symbol=%s"
        stock_info_df = pd.read_sql(stock_info_sql, con=conn, params=[request_body.get("symbol")])

        result = get_data_list_from_df_paging(request_body, stock_info_df, hist_data_df)

        return result
    
    except Exception as e:
        logger.error(e)
        logger.error(str(e))
        raise e

def get_data_list_from_df_paging(request_body, stock_info_df, hist_data_df):
    all_data_result = get_data_list_from_df(stock_info_df, hist_data_df)

    page = 1
    if None != request_body.get("page"):
        page = int(request_body.get("page"))
    
    limit = 60
    if None != request_body.get("limit"):
        limit = int(request_body.get("limit"))
    
    offset = limit
    if None != request_body.get("offset"):
        offset = int(request_body.get("offset"))

    # 看看翻页的总长度是否超过data的长度
    len_db = len(hist_data_df)
    all_data_result["len_total_data"] = len_db

    start_number = (page - 1) * limit
    end_number = (page - 1) * limit + offset
    print(f"start: {start_number}, end: {end_number}, total_db: {len_db}")

    if start_number > len_db:
        print(f"INFO: there's not enough data to do paging. the start number is {start_number} while the total len of DB data is {end_number}")
        all_data_result["paging_datas"] = []
        return all_data_result

    if end_number > len_db:
        end_number = len_db

    hist_df_after_sort = hist_data_df.sort_values("trade_date",ascending=0)

    hist_df_after_paging = df_paging(hist_df_after_sort, start_number, end_number)
    print(hist_df_after_paging)

    paging_data_result = get_data_list_from_df(stock_info_df, hist_df_after_paging)

    all_data_result["paging_datas"] = paging_data_result.get("datas")

    return all_data_result


def df_paging(df, start, end):
    """
    传入参数: 
    limit: int,  必须, 默认为20, 每页显示的数据量
    page: int, 必须, 默认为1, 第几页
    offset: int, 可选,默认等值于limit, 偏移量
    """
    df2 = df[start: end]
    return df2

def get_data_list_from_df(stock_info_df, hist_data_df):
    result = {}

    result["name"] = stock_info_df.iat[0, 2]

    hist_data_df["trade_date"] = pd.to_datetime(hist_data_df["trade_date"])
    hist_data_df["trade_date"] = [x.strftime('%Y/%m/%d') for x in hist_data_df['trade_date']]

    hist_data_df["open"] = hist_data_df["open"].astype("float")
    hist_data_df["close"] = hist_data_df["close"].astype("float")
    hist_data_df["low"] = hist_data_df["low"].astype("float")
    hist_data_df["high"] = hist_data_df["high"].astype("float")

    ll = hist_data_df.values.tolist()

    result["datas"] = ll

    return result

def transfer_from_symbol(symbol):
    number_s = symbol[2:]
    ts = symbol[:2]

    return number_s + "." + ts.upper()

def get_number_from_symbol(symbol):
    return symbol[2:]

def getMapFromDf(df):
    result_list = []

    df["ts_code"] = df["ts_code"].astype("str")
    df["symbol"] = df["symbol"].astype("str")

    df["symbol"] = df["ts_code"].str.lower() + df["symbol"]

    key_l = df.columns
    
    for e in df.values:
        t = dict(zip(key_l, e))
        result_list.append(t)
    
    return result_list

def createStocksTable():
    try:
        conn = getConnection()

        datas = getStockData()
        # datas = pd.DataFrame({"北京":[100,300,180],"上海":[108,30,70],"武汉":[170,90,280]})

        print(datas)
        
        # result = datas.to_sql("stocks", conn, index=True, if_exists="replace")

        return datas
        
    except Exception as e:
        logger.error(e)
        logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")
        raise e

def getStockData():
    apiInfo = apiBasicInfo()

    httpApi = api_http(apiInfo["http_api"], apiInfo["token"], apiInfo["protocol"])

    apiName = "stock_basic"

    df_L = httpApi.query(apiName, fields='ts_code,symbol,name,area,industry,list_date,delist_date,market,is_hs')
    df_D = httpApi.query(apiName, fields='ts_code,symbol,name,area,industry,list_date,delist_date,market,is_hs', list_status="D")
    df_P = httpApi.query(apiName, fields='ts_code,symbol,name,area,industry,list_date,delist_date,market,is_hs', list_status="P")

    df_t = pd.concat([df_L, df_D, df_P])

    df_t["ts_code"] = df_t["ts_code"].str[-2:]  # SZ replace 000001.SZ

    return df_t
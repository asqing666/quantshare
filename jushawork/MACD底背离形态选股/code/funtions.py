import talib
from talib import MA_Type
import pandas as pd
import datetime
import pandas as pd
import os
import numpy as np
import tushare as ts
import datetime
from datetime import timedelta
pd.set_option('display.max_columns', 100)
import warnings
warnings.filterwarnings("ignore")

# MACD顶底背离金叉与死叉
def MACD_Deviation(df):
    open_p = df['Open'].values
    high_p = df['High'].values
    close_p = df['Close'].values
    low_p = df['Low'].values
    volume = df["Volume"].values
    volume_p = df["Volume"].values
    df["MACD_DIF"], df["MACD_DEA"], df["MACD_Hist"] = talib.MACD(close_p, fastperiod=12, slowperiod=26, signalperiod=9)
    df['JCHA'] = np.where((df["MACD_Hist"] > 0) & (df["MACD_Hist"].shift(1) < 0), 1, 0)  # type: np.ndarray
    df['SCHA'] = np.where((df["MACD_Hist"] < 0) & (df["MACD_Hist"].shift(1) > 0), 1, 0)  # type: np.ndarray

    # 判断底背离
    jcha_data = df[df['JCHA'] == 1]
    jcha_data.reset_index(drop=True, inplace=True)

    jcha_data['pct_chg'] = 100 * jcha_data['Close'].pct_change()
    jcha_data['DIF_chg'] = 100 * jcha_data['MACD_DIF'].pct_change()
    jcha_data['FLAG'] = None
    for i in range(1, len(jcha_data)):
        if i >= len(jcha_data):
            pass
        # 构造有效金叉
        elif (pd.to_datetime(jcha_data.loc[i, 'Date']) - pd.to_datetime(jcha_data.loc[i - 1, 'Date'])).days < 20:
            jcha_data.drop(index=[i], inplace=True)
            jcha_data.reset_index(drop=True, inplace=True)
            if i >= len(jcha_data):
                pass
            else:
                date_0 = jcha_data.loc[i - 1, 'Date']
                date_1 = jcha_data.loc[i, 'Date']
                df_1 = df[(df['Date'] >= date_0) & (df['Date'] <= date_1)]
                if len(df_1[df_1['MACD_DEA'] > 0]):
                    jcha_data.loc[i, 'FLAG'] = 0
                else:
                    jcha_data.loc[i, 'FLAG'] = 1
        else:
            date_0 = jcha_data.loc[i - 1, 'Date']
            date_1 = jcha_data.loc[i, 'Date']
            df_1 = df[(df['Date'] >= date_0) & (df['Date'] <= date_1)]
            if len(df_1[df_1['MACD_DEA'] > 0]):
                jcha_data.loc[i, 'FLAG'] = 0
            else:
                jcha_data.loc[i, 'FLAG'] = 1
    jcha_data['JCHA_FLAG'] = np.where(((jcha_data['pct_chg'] <= 2) &
                                       ((abs(jcha_data['pct_chg']) >= abs(jcha_data['DIF_chg'])) | (
                                                   jcha_data['MACD_DIF'] >= jcha_data['MACD_DIF'].shift(1)))
                                       & (jcha_data['FLAG'] == 1)
                                       ), 1, 0)
    del jcha_data['FLAG']

    # 判断顶背离
    scha_data = df.loc[df['SCHA'] == 1]
    scha_data.reset_index(drop=True, inplace=True)
    scha_data['pct_chg'] = 100 * scha_data['Close'].pct_change()
    scha_data['DIF_chg'] = 100 * scha_data['MACD_DIF'].pct_change()
    scha_data['FLAG'] = None
    # 有效的金叉与死叉的构建还存在问题，有效金叉是指MACDhist能有效稳定持续3天或者5天左右，如果小于3天，则认为是无效金叉/死叉
    for i in range(1, len(scha_data)):
        if i >= len(scha_data):
            pass
        # 构造有效死叉
        elif (pd.to_datetime(scha_data.loc[i, 'Date']) - pd.to_datetime(scha_data.loc[i - 1, 'Date'])).days < 20:
            scha_data.drop(index=[i], inplace=True)
            scha_data.reset_index(drop=True, inplace=True)
            if i >= len(scha_data):
                pass
            else:
                date_0 = scha_data.loc[i - 1, 'Date']
                date_1 = scha_data.loc[i, 'Date']
                df_1 = df[(df['Date'] >= date_0) & (df['Date'] <= date_1)]
                if len(df_1[df_1['MACD_DEA'] < 0]):
                    scha_data.loc[i, 'FLAG'] = 0
                else:
                    scha_data.loc[i, 'FLAG'] = 1
        else:
            date_0 = scha_data.loc[i - 1, 'Date']
            date_1 = scha_data.loc[i, 'Date']
            df_1 = df[(df['Date'] >= date_0) & (df['Date'] <= date_1)]
            if len(df_1[df_1['MACD_DEA'] < 0]):
                scha_data.loc[i, 'FLAG'] = 0
            else:
                scha_data.loc[i, 'FLAG'] = 1

    scha_data['SCHA_FLAG'] = np.where(((scha_data['pct_chg'] >= -2) &

                                       ((abs(scha_data['pct_chg']) >= abs(scha_data['DIF_chg'])) | (
                                                   scha_data['MACD_DIF'] <= scha_data['MACD_DIF'].shift(1)))
                                       & (scha_data['FLAG'] == 1)
                                       ), 1, 0)
    del scha_data['FLAG']

    return jcha_data, scha_data

# 将tushare日期转换成固定格式
def str_to_date(int):
    date = str(int)
    if len(date) == 8:
        return date[0:4] + '-' + date[4:6] + '-' + date[6:8]
    else:
        return date

# 转回无 - 的str模式的date
def date_to_str(x):
    date = x
    if len(date) == 10:
        return date[0:4]  + date[5:7] + date[8:10]
    else:
        return date

# 获取正常上市的所有股票代码
def get_allstock_code(pro,exchange='', list_status='L', fields='ts_code'):
    data = pro.stock_basic(exchange=exchange, list_status=list_status, fields=fields)
    return data

# 获取股票数据
def get_stock_data(pro,code, start, end):
    stock_df = pro.daily(ts_code=code, start_date=start, end_date=end)
    stock_df['trade_date'] = stock_df['trade_date'].apply(str_to_date)
    stock_df.sort_values(['trade_date'],inplace=True)
    stock_df.reset_index(drop=True,inplace=True)
    return stock_df


# 股价复权+计算技术性指标因子
def fuquan(df):
    df['pct_chg'] = 100 * (df['Close'] / df['pre_close'] - 1).round(4)
    # 股票复权
    df['fuquan'] = (1 + df['pct_chg'] / 100).cumprod()
    # close 是没复权的，Close是复权
    df['close'] = df['Close']
    df['Close'] = (df['fuquan'] * (df.iloc[0]['Close'] / df.iloc[0]['fuquan'])).round(2)

    df['Open'] = (df['Open'] / df['close'] * df['Close']).round(2)
    df['High'] = (df['High'] / df['close'] * df['Close']).round(2)
    df['Low'] = (df['Low'] / df['close'] * df['Close']).round(2)

    del df['close']

    # 分别计算当天之后 5 ，10 ，20天的涨跌幅
    df['pct_chg_5'] = (100 * (df['Close'].shift(-5) / df['Close'] - 1)).round(2)
    df['pct_chg_10'] = (100 * (df['Close'].shift(-10) / df['Close'] - 1)).round(2)
    df['pct_chg_20'] = (100 * (df['Close'].shift(-20) / df['Close'] - 1)).round(2)
    df['pct_chg_30'] = (100 * (df['Close'].shift(-30) / df['Close'] - 1)).round(2)

    # 计算90日bias
    df['mean_90'] = df['Close'].rolling(90).mean()
    df['bias_90'] = 100 * (df['Close'] / df['mean_90'] - 1)

    # 计算5日均线以及60日均线，当5日均线上穿60日均线，为买入信号
    df['mean_5'] = df['Close'].rolling(5).mean()
    df['mean_60'] = df['Close'].rolling(60).mean()
    df['5upcross60'] = 100 * (df['mean_5'] / df['mean_60'] - 1).round(4)

    # 计算MA60与MA90的偏离程度
    df['diff_MA60-MA90'] = 100 * (df['mean_60'] / df['mean_90'] - 1).round(4)

    del df['mean_5']
    del df['mean_60']
    del df['mean_90']
    # 计算成交量
    df['Vol_5'] = df['Volume'].rolling(5).mean()
    # 计算RSI
    close_p = df['Close'].values
    df["RSI"] = talib.RSI(close_p, timeperiod=14)
    # 计算当日最高点与收盘价的回撤
    df['now_high_close'] = 100 * (df['Close'] / df['High'] - 1).round(4)
    # 计算股票波动率
    df['std_60'] = round(100 * (df['Close'].rolling(50).std() / df['Close'].rolling(50).mean()), 4)

    # 30天内的最高点的回撤
    df['30_down'] = 100 * (df['Close'] / df['High'].shift(1).rolling(20).max() - 1).round(4)


    # 计算一年半内最高点与当日的一个最大回撤
    #     df['max_down'] = 100*(df['Close']/df['Close'].rolling(365,min_periods=90).max()-1).round(4)

    return df


# 出现底背离金叉的时候，先不着急进场，等价格突破均线再进场，然后加各种过滤条件
def wait_chance(df, df_j, df_s):
    date_list = df_j[df_j['JCHA_FLAG'] == 1].Date.values.tolist()

    # 计算底背离后面走势，走出90bias>0的那天
    trade_flag = []
    for i in range(0, len(date_list)):
        if i == len(date_list) - 1:
            df1 = df[(df['Date'] >= date_list[i])]
        else:
            df1 = df[(df['Date'] >= date_list[i]) & (df['Date'] <= date_list[i + 1])]
        nice_date = df1[df1['5upcross60'] >= 0]['Date'].tolist()
        if len(nice_date):
            trade_flag.append(nice_date[0])
    df.loc[(df['Date'].isin(trade_flag)), 'BUY_FLAG'] = 1

    # 再次筛选，涨幅不大，量能不大地突破均线的底背离
    # 去掉突破形式为一字板的情况,最高点等于最低点
    df.loc[((df['BUY_FLAG'] == 1) & (df['High'] == df['Low'])), 'BUY_FLAG'] = 0
    buy_date = df[df['BUY_FLAG'] == 1].Date.unique()
    ture_date = []
    for day in buy_date:
        day_20 = (pd.to_datetime(day) - timedelta(days=30)).strftime('%Y-%m-%d')
        #         day_10 = (pd.to_datetime(day)-timedelta(days=10)).strftime('%Y-%m-%d')

        #         # 10天前看量能
        #         df10 = df[((df['Date']<day) & (df['Date']>day_10))]
        #         df10['vol_itude'] = df10['Volume']/df10['Vol_5']

        # 20天前看最大涨幅
        df20 = df[((df['Date'] <= day) & (df['Date'] >= day_20))]
        df20['min'] = df20['Low'].expanding().min()
        df20['max_chg'] = 100 * (df20['Close'] / df20['min'] - 1)
        df20['vol_itude_20'] = df20['Volume'] / df20['Vol_5']

        if (len(df20[df20['vol_itude_20'] > 2.3]) | len(df20[df20['max_chg'] > 22])):
            pass
        else:
            ture_date.append(day)
    df['trade_type'] = None
    if len(ture_date):
        df.loc[(df['Date'].isin(ture_date)), 'trade_type'] = 1
    else:
        pass
    #     buy_df = df[['ts_code','Date','trade_type','pct_chg','pct_chg_5','pct_chg_10','pct_chg_20','pct_chg_30','RSI','5upcross60','now_high_close','std_60','30_down','diff_MA60-MA90',
    #                 ]]

    buy_df = df[
        ['ts_code', 'Date', 'trade_type', 'pct_chg', 'pct_chg_5', 'pct_chg_10', 'pct_chg_20', 'pct_chg_30', 'RSI',
         '5upcross60', 'now_high_close', 'std_60', '30_down', 'diff_MA60-MA90']]

    # 出现买入信号
    buy_df = buy_df[buy_df['trade_type'] == 1]

    # 各种筛选条件
    # RSI<67
    buy_df = buy_df[buy_df['RSI'] <= 67]

    # 当日涨幅要大于0.2%，小于8%
    buy_df = buy_df[buy_df['pct_chg'] > 0.2]
    buy_df = buy_df[buy_df['pct_chg'] < 8.0]

    # 当日回撤要大于-4%
    buy_df = buy_df[buy_df['now_high_close'] > -4.0]

    # 30天内从最高点回撤要大于-6.20%,小于3.55%
    buy_df = buy_df[buy_df['30_down'] > -6.20]
    buy_df = buy_df[buy_df['30_down'] < 3.55]

    # 60日内波动率要小于7%，大于1%
    buy_df = buy_df[buy_df['std_60'] < 7.0]
    buy_df = buy_df[buy_df['std_60'] > 1.0]

    # MA60与MA90的间隔要大于-12.2%
    buy_df = buy_df[buy_df['diff_MA60-MA90'] > -12.2]

    # 一年半内的至今与当日比较最大回撤要>20%
    #     buy_df = buy_df[buy_df['max_down']<-20]

    #     buy_df.dropna(inplace=True)

    return buy_df
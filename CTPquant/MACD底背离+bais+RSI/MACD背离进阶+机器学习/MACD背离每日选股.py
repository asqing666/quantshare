from funtions import *

if __name__ == '__main__':
    # 初始化数据
    start = '20200520'
    end = '20210619'
    name_dict = {'trade_date': 'Date', 'open': 'Open', 'high': 'High',
                 'low': 'Low', 'close': 'Close', 'vol': 'Volume',
                 'amount': 'Value',
                 }

    # 获取股票列表
    df_code = get_allstock_code(exchange='', list_status='L', fields='ts_code, name')
    # 只选要00，60开头的，不要st
    df_code = df_code[df_code['name'].str.contains('ST') == False]
    df_code = df_code[df_code['ts_code'].str.startswith('68') == False]
    df_code = df_code[df_code['ts_code'].str.startswith('300') == False]
    df_code.reset_index(drop=True, inplace=True)

    # 获取符合形态条件的票
    all_jchadata = pd.DataFrame()
    for code in df_code.ts_code:
        print(code)
        df = get_stock_data(code, start, end)
        # 过滤掉上市未满一年的股票
        if (len(df) > 200):
            df.rename(columns=name_dict, inplace=True)  # 列重命名
            df = fuquan(df)
            df_j, df_s = MACD_Deviation(df)  # 判断顶底背离函数
            buy_df = wait_chance(df, df_j, df_s)  # 出现顶底背离后，后期满足一点条件再进场
            all_jchadata = all_jchadata.append(buy_df)
    print(all_jchadata)

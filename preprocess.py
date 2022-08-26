# encoding: utf-8
"""
@author: luoyang
@time: 2022/8/26 4:05 PM
@desc:
"""
import pandas as pd
import os
import datetime
def delta_days(reg_timestamp, latest_timestamp):
    reg_time = datetime.datetime.strptime(reg_timestamp,"%Y-%m-%d")
    latest_time = datetime.datetime.strptime(latest_timestamp,"%Y-%m-%d %H:%M:%S")
    delta = (latest_time - reg_time).days
    return delta

def process(filename):
    header_names = ['dt', 'uid', 'latest_login', 'anchor_type', 'area', 'total_dollar',
                    'reg_date', 'kanbo_pv', 'follow_uv', 'consume_coin_for_live', 'consume_coin_total']


    pd.set_option("display.max_columns",None)
    df = pd.read_csv(filename,header=0, names=header_names)
    df.fillna(value=0, inplace=True)

    encode_names = ['anchor_type', 'area', ]
    encode_dic = {}
    decode_dic = {}
    for name in encode_names:
        keys = df[name].value_counts().keys().to_list()
        tmp = {}
        cnt = 0
        for key in keys:
            tmp[key] = cnt
            cnt += 1
        encode_dic[name] = tmp
        decode_dic[name] = {v: k for k, v in tmp.items()}

    df["delta_days"] = df.apply(lambda row: delta_days(row['reg_date'], row['latest_login']), axis=1)
    #todo df["total_dollar_e"] = df.apply(lambda row: )
    
    df["anchor_type_e"] = df["anchor_type"].map(encode_dic['anchor_type'])
    df["area_e"] = df["area"].map(encode_dic['area'])

    df_processed = df['uid','area','anchor_type_e','']

    return encode_dic, decode_dic

def gen_map_dics(dataframe, cols):
    for col in cols:
        counts = dataframe[col].value_counts()


if __name__ == '__main__':
    crt_path = os.getcwd()
    filename = os.path.join(crt_path, "query-hive-75432.csv")
    process(filename)
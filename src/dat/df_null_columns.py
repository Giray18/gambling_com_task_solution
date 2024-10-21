import pandas as pd
import numpy as np

# def df_null_columns(df: pd.DataFrame):
#     ''' Returns full null value column names'''
#     # df = df.replace('None','')
#     null_val_holding_cols = [i for i in df.columns if df[i].isnull().all() == True]
#     # null_val_holding_cols = [i for i in df.columns if df[i].isna().sum() == df[i].count()]
#     d = {'null_cols' : null_val_holding_cols}
#     df_null_val_holding_cols = pd.DataFrame(data=d)
#     # df_null_val_holding_cols = pd.DataFrame([null_val_holding_cols],columns = df.columns)
#     return df_null_val_holding_cols

# if __name__ == '__main__':
#     df_null_columns()




def df_null_columns(df: pd.DataFrame, percentage = 0.05):
    ''' Returns dataframe that shows field values that takes 
    more than %5 of total values on a field'''
    #Created an empty dict to append series for out concat operation
    dict_1 = {}
    global df_ultimate
    df = df.select_dtypes(include=["string","object"])
    if int(df.size) < 0:
        df_ultimate = pd.DataFrame(['No value has dominance more than %5 of column values on dataset'],columns = ["describe_non_numeric"])
    else:
        for i in df.columns:
            # if df[i].dtype in ["string","object"]:
            s = df[i].explode().value_counts(normalize=True)
            # Filtered values does not have more than %5 of all values in a particular column
            s = s.loc[lambda x : x >= percentage]
            dict_1[i] = []
            dict_1.update({i:s})
            # Concat all series we created into one
            df_ultimate = pd.concat(dict_1, axis = 1)
            # Removed all null columns
            df_ultimate = df_ultimate.loc[:,df_ultimate.notna().any(axis=0)]
            # For newer versions of pandas > 2.1 first line is valid for lower versions one line below is valid code
            # df_ultimate = df_ultimate.astype(float).map('{:,.1%}'.format)
            # df_ultimate = df_ultimate.transform(lambda x : x.map('{:,.1%}'.format))
            df_ultimate = df_ultimate.filter(items=['<NA>'],axis=0)
            df_ultimate = df_ultimate.dropna(axis='columns')
            df_ultimate = df_ultimate.loc[:, ~(df_ultimate != 1).any()]
            # df_ultimate = df_ultimate.filter(items=['1'],axis='columns')
    return df_ultimate



if __name__ == '__main__':
    df_null_columns()
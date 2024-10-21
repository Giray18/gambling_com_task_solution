import pandas as pd
import xlsxwriter
import openpyxl
import numpy as np


# def save_dataframe_excel(df_list = {},file_name = "data_analysis"):
#     ''' Saves all active dataframes into an excel sheet 
#     created in session'''
#     # global writer
#     writer = pd.ExcelWriter(f"{file_name}.xlsx", engine="xlsxwriter")
#     for i in df_list:
#         # writer = pd.ExcelWriter(f"{file_name}.xlsx", engine="xlsxwriter")
#         # df_list[i].astype('str').dtypes
#         if type(df_list[i]) == pd.DataFrame and i != "df":
#             # print(i)
#             # df_list[i] = df_list[i].astype('str').dtypes
#             # df_list[i].select_dtypes(include=[np.DatetimeTZDtype])
#             df_list[i].astype('str').dtypes.to_excel(writer, sheet_name = i)
#     writer.close() 
#     return "saved to excel"

def save_dataframe_excel(df_com ,file_name = "data_analysis"):
    ''' Saves all active dataframes into an excel sheet 
    created in session'''
    # global writer
    writer = pd.ExcelWriter(f"{file_name}.xlsx", engine="xlsxwriter")
    # for i in df_list:
        # writer = pd.ExcelWriter(f"{file_name}.xlsx", engine="xlsxwriter")
        # df_list[i].astype('str').dtypes
        # if type(df_list[i]) == pd.DataFrame and i != "df":
            # print(i)
            # df_list[i] = df_list[i].astype('str').dtypes
            # df_list[i].select_dtypes(include=[np.DatetimeTZDtype])
            # df_list[i].astype('str').dtypes.to_excel(writer, sheet_name = i)
    df_com.astype('str').dtypes.to_excel(writer, sheet_name = i)      
    writer.close() 
    return "saved to excel"
    


if __name__ == '__main__':
    save_dataframe_excel()
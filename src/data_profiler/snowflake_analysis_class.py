import snowflake.connector
import time
import datetime
import re
import os
from collections import defaultdict
import pandas as pd
import numpy as np
import sys
sys.path.insert(0, 'C:/Users/Lenovo/Desktop/gambling/gambling_com_task_solution/src')

class snowflake_analysis_class:
    '''A python class to create quick profiling of tables located on snowflake
    addition to data profiling and manipulation capabilities of class for pandas dataframes'''

    def __init__(self, user, password, account, warehouse, database, schema, authenticator) -> None:
        self.user = user
        self.password = password
        self.account = account
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.authenticator = authenticator


    def multiple_dataset_apply_containing_cols_snwflk(self, valuesearch, view_or_table = 'TABLES'):
        '''Detects columns that contains each other on entire database and saves as output file'''
        # Frames empty list to be used in further pandas dataframe creation operation
        global frames
        frames = []
        ####
        ctx = snowflake.connector.connect(
        user = f'{self.user}',
        password = f'{self.password}',
        account = f'{self.account}',
        warehouse = f'{self.warehouse}',
        database = f'{self.database}',
        schema = f'{self.schema}',
        authenticator = f'{self.authenticator}',
        )
        # Create a cursor from the connection variable
        cursor = ctx.cursor()

        #Reading table names on database into read_tbl list
        cursor.execute(f"SHOW {view_or_table}")
        myresult = cursor.fetchall()
        # saving all tables of database into a list
        read_tbl = [table_name[1] for table_name in myresult]
        dataframes_dict = {}
        for f in read_tbl:
            main_df = pd.DataFrame()
            df_detect = pd.read_sql(f'SELECT * FROM {f} LIMIT 100',con=ctx).dropna().select_dtypes(exclude='boolean')
            df_detect = df_detect.loc[:, ~(df_detect == True).any()]
            df_detect = df_detect.loc[:, ~(df_detect == False).any()]
            col_check = df_detect.columns
            for col in col_check:
                    try:
                        df = pd.read_sql(f"SELECT {col} FROM {f} WHERE {col} = '{valuesearch}'", con=ctx)
                        main_df = pd.concat([df,main_df])
                    except Exception as e:
                        continue

            if main_df.size != 0:
                dataframes_dict[f] = main_df

        for key, value in dataframes_dict.items():
            for key_col, columnData in value.items():
                occurence = len(columnData)
                if columnData.isin([valuesearch]).any() == True:
                    d = {"table_1": [key], "col_1": [key_col], "sample" : [valuesearch], "occurence" : [occurence]}
                    frames.append(d)
        # Creating working directory for daily partitioning of output files
        dir = os.path.join(
            "C:\\", "Users/cihan.oner/GitRepos/snowflake_vs_code/power_bi_reporting_solution/snwflke_profiling_res", f'{date.today()}_containing_cols')
        if not os.path.exists(dir):
            os.mkdir(dir)
        os.chdir(dir)
        # Writing to xlsx file
        writer = pd.ExcelWriter(
            f"value_search_{date.today()}_{self.schema}_{valuesearch}.xlsx", engine="xlsxwriter")
        df_ultimate = pd.DataFrame(frames)
        if df_ultimate.size != 0:
            df_ultimate.to_excel(writer, sheet_name="containing_cols")
            writer.close()
        return "output files saved for containing columns"
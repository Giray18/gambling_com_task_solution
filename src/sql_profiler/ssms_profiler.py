import snowflake.connector
import time
from datetime import datetime, timedelta, date
import re
import os
import json
import glob
from collections import defaultdict
import pandas as pd
import numpy as np
import sys
sys.path.insert(0, 'C:/Users/cihan.oner/GitRepos/snowflake_vs_code/power_bi_reporting_solution/src')
import dat


class ssms_profiler:
    '''A python class to create quick profiling of tables located on mysql server'''


    def __init__(self, user, password, account, warehouse, database, schema, authenticator) -> None:
        self.user = user
        self.password = password
        self.account = account
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.authenticator = authenticator

    def multiple_dataset_apply_profiling(self, view_or_table = 'TABLES') -> pd.DataFrame: 
        ''' This function reads multiple tables from connected database  
        then runs all data profiling functions from dat package makes data profiling and
        saves outputs into an excel file on defined working directory '''

        # Creating connection variable
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
        read_tbl = [table_name[1] for table_name in myresult]

        # Creating a dict consisting of all dataframes
        dataframes_dict = {}

        # Creating working directory for daily partitioning
        dir = os.path.join(
            "C:\\", "Users/cihan.oner/GitRepos/snowflake_vs_code/power_bi_reporting_solution/snwflke_profiling_res", f'{date.today()}_{self.schema}')
        if not os.path.exists(dir):
            os.mkdir(dir)
        os.chdir(dir)

        # loop over the list of sql tables and read them into pandas dataframe
        for f in read_tbl:
            # Limited to 1 million max transaction
            df = pd.read_sql(f'SELECT * FROM {f} LIMIT 1000000', con=ctx)
            # df = df.replace({None : np.nan})
            df = df.replace({None : pd.NA})
            # print(df)
            # Convert timestamp to strings
            # date_cols = df.select_dtypes(include=['datetime64[ns]','datetime64[ns, UTC]']).columns
            # print(date_cols)
            # for column in date_cols:
            #     df[column] = df[column].astype('str')
            #     print(df[column])
            # Convert bytearray to strings
            # for col, dtype in df.dtypes.items():
            #     if dtype == str:
            #         try:
            #             df[col] = df[col].str.decode('utf-8').fillna(df[col])
            #         except UnicodeDecodeError or TypeError:
            #             df[col] = 'bytearray_column'
            # Creating a dict consisting of all dataframes
            # if df.size != 0:
            #     dataframes_dict[f] = df
            # Applying methods to dataframes defined on analysis_dict.py file
            writer = pd.ExcelWriter(f"{f}.xlsx", engine="xlsxwriter")
            for key, value in dat.analysis_dict().items():
                if df.size != 0 and key != 'df':
                    # vars()[key] = value(df)
                    # value(df).astype('str').dtypes
                    # Fix for byte codes and timezone holding cols
                    try:
                        value(df).to_excel(writer, sheet_name = key)
                    except TypeError:
                        df_sec = df
                        for col in df_sec:
                            df_sec[col] = df_sec[col].astype('str')
                        df_write = value(df_sec)
                        df_write.to_excel(writer, sheet_name = key)
                    except ValueError:
                        df_sec = df
                        for col in df_sec:
                            df_sec[col] = df_sec[col].astype('str')
                        df_write = value(df_sec)
                        df_write.to_excel(writer, sheet_name = key)
                        # for col, dtype in value(df).dtypes.items():
                        # continue
                else:
                    continue
                    # value(df).to_excel(writer, sheet_name = key)
            writer.close()
        return 'dataframes_profiled_results_saved_into_working_directory'
        # return dataframes_dict


    # def data_dict_crate_sqlite(database_name:str):
    def data_dict_crate_snwflk(self) -> pd.DataFrame: 
        ''' This function reads multiple tables from connected database and creates
        data dictionary of all columns exists on tables '''
        #Regex pattern for columns
        pattern_d = re.compile(r"[0-9]{4}.[0-9]{2}.[0-9]{2}.*", re.IGNORECASE)
        pattern_d_alt = re.compile(r"[0-9]{2}.[0-9]{2}.[0-9]{4}.*", re.IGNORECASE)
        pattern_a_numeric = re.compile(r"[A-Za-z0-9]+", re.IGNORECASE)
        pattern_text = re.compile(r"[A-Za-z]+", re.IGNORECASE)
        pattern_float = re.compile(r"[0-9]*\.[0-9]+", re.IGNORECASE)
        pattern_int = re.compile(r"[0-9]+", re.IGNORECASE)
        pattern_phone = re.compile(r"\+[0-9]+.*", re.IGNORECASE)
        pattern_email = re.compile(r"[A-Za-z0-9]+@", re.IGNORECASE)

        # use glob to get all sql tables
        # in the folder
        # con = sqlite3.connect(database_name)
        # # con.text_factory = lambda x: str(x, encoding)
        # con.text_factory = lambda b: b.decode(errors = 'ignore')
        # cur = con.cursor()
        # cur.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        # # saving all tables of database into a list
        # read_table_names = [table_name[0] for table_name in cur]

        # Creating connection variable
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
        cursor.execute("SHOW TABLES")
        myresult = cursor.fetchall()
        read_table_names = [table_name[1] for table_name in myresult]

        global frames
        frames = []
        
        for f in read_table_names:
            # read the sql tables by condition of date-time column hold
            df_detect = pd.read_sql(f'SELECT * FROM {f} LIMIT 1', ctx)
            # for i in df_detect.columns:
                # try:
                #     col_date = set([df_detect[i].name for i in df_detect.columns if pattern_d.match(str(df_detect[i].iloc[0])) or pattern_d_alt.match(str(df_detect[i].iloc[0]))])
                #     print(col_date)
                # except IndexError:
                #     continue
            # Getting list of date-time columns from detection dfs
            try:
                col_date = [df_detect[i].name for i in df_detect.columns if pattern_d.match(str(df_detect[i].iloc[0])) or pattern_d_alt.match(str(df_detect[i].iloc[0]))]
            except IndexError:
                continue
            col_a_num = [df_detect[i].name for i in df_detect.columns if pattern_a_numeric.match(str(df_detect[i].iloc[0]))]
            col_text = [df_detect[i].name for i in df_detect.columns if pattern_text.match(str(df_detect[i].iloc[0]))]
            col_float = [df_detect[i].name for i in df_detect.columns if pattern_float.match(str(df_detect[i].iloc[0]))]
            col_int = [df_detect[i].name for i in df_detect.columns if pattern_int.match(str(df_detect[i].iloc[0]))]
            col_phone = [df_detect[i].name for i in df_detect.columns if pattern_phone.match(str(df_detect[i].iloc[0]))]
            col_email = [df_detect[i].name for i in df_detect.columns if pattern_email.match(str(df_detect[i].iloc[0]))]
            for k,v in df_detect.items():
                if k in col_date:
                    d = {"column" : [k], "table" : [f], "dtype" : "date" , 'value' : [v][0]}
                    df_output = pd.DataFrame(data=d)
                    for column in df_output:
                        df_output[column] = df_output[column].astype('str')
                    frames.append(df_output)
                elif k in col_phone:
                    d = {"column" : [k], "table" : [f], "dtype" : "phone_num" , 'value' : [v][0]}
                    df_output = pd.DataFrame(data=d)
                    for column in df_output:
                        df_output[column] = df_output[column].astype('str')
                    frames.append(df_output)
                elif k in col_email:
                    d = {"column" : [k], "table" : [f], "dtype" : "email" , 'value' : [v][0]}
                    df_output = pd.DataFrame(data=d)
                    for column in df_output:
                        df_output[column] = df_output[column].astype('str')
                    frames.append(df_output)
                elif k in col_text:
                    d = {"column" : [k], "table" : [f], "dtype" : "text" , 'value' : [v][0]}
                    df_output = pd.DataFrame(data=d)
                    for column in df_output:
                        df_output[column] = df_output[column].astype('str')
                    frames.append(df_output)
                elif k in col_float:
                    d = {"column" : [k], "table" : [f], "dtype" : "float" , 'value' : [v][0]}
                    df_output = pd.DataFrame(data=d)
                    for column in df_output:
                        df_output[column] = df_output[column].astype('str')
                    frames.append(df_output)
                elif k in col_int:
                    d = {"column" : [k], "table" : [f], "dtype" : "int" , 'value' : [v][0]}
                    df_output = pd.DataFrame(data=d)
                    for column in df_output:
                        df_output[column] = df_output[column].astype('str')
                    frames.append(df_output)
                elif k in col_a_num:
                    d = {"column" : [k], "table" : [f], "dtype" : "a_numeric" , 'value' : [v][0]}
                    df_output = pd.DataFrame(data=d)
                    for column in df_output:
                        df_output[column] = df_output[column].astype('str')
                    frames.append(df_output)
            df_ultimate = pd.concat(frames)
        dir = os.path.join("C:\\", "Users/cihan.oner/GitRepos/snowflake_vs_code/power_bi_reporting_solution/snwflke_profiling_res", f'{date.today()}_{self.schema}_data_dict')
        if not os.path.exists(dir):
            os.mkdir(dir)
        os.chdir(dir)
        # Saving containing cols dataframe 
        writer = pd.ExcelWriter(f"data_dict_{date.today()}_{self.schema}.xlsx", engine="xlsxwriter")
        df_ultimate.to_excel(writer,sheet_name="data_dict")
        writer.close()
        return "data dict saved"

    def apply_sql_query(self, sql_command: str) -> pd.DataFrame:
        ''' This function gets sql statement as text input and runs it through connected 
         SQL db and saves result output into an excel file then into defined workind directory '''
        # Creating connection variable
        cnxn = pyodbc.connect(f'DSN={self.DSN};PWD={self.password}')

        # Create sql query and save results into pandas dataframe
        query_results = pd.read_sql(f'{sql_command}', con=cnxn)

        # Creating working directory for saving output into a file with daily partitioning
        dir = os.path.join(
            "C:\\", "Users/Lenovo/Desktop/acronis test/task/power_bi_reporting_solution/sql_query_results", f'{date.today()}')
        if not os.path.exists(dir):
            os.mkdir(dir)
        os.chdir(dir)

        # Saving pandas dataframe into created working directory
        if query_results.size != 0:
            sql_command = sql_command.replace("*", "ALL") # Saving * not possible in file name
            writer = pd.ExcelWriter(f"{sql_command[0:10]}_{date.today()}.xlsx", engine="xlsxwriter")
            query_results.to_excel(writer, sheet_name = 'sql_command')
            writer.close()
            return query_results
        else:
            return 'query results are empty'

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
            # davar = ctx.cursor()
            # cols = davar.execute(f"SHOW COLUMNS LIKE '%FUND%'").fetchall()
            # cols_itarate = [table_name[2] for table_name in cols]
            # df = pd.read_sql(f"SELECT * FROM {f} ORDER BY _FIVETRAN_SYNCED DESC LIMIT 100000",con=ctx)
            # df_1 = pd.read_sql(f"SELECT * FROM {f}  WHERE _FIVETRAN_SYNCED LIKE '{date_1}%'",con=ctx)
            # df = pd.read_sql(f"SELECT * FROM {f}  WHERE _FIVETRAN_SYNCED LIKE '{date_2}%'",con=ctx)
            df_detect = pd.read_sql(f'SELECT * FROM {f} LIMIT 100',con=ctx).dropna().select_dtypes(exclude='boolean')
            # df_detect = df_detect.replace({None : "monkey"})
            # df_detect = df_detect.replace({False : "monkey"})
            # df_detect = df_detect.replace({True : "monkey"})
            df_detect = df_detect.loc[:, ~(df_detect == True).any()]
            df_detect = df_detect.loc[:, ~(df_detect == False).any()]
            # col_drop = [df_detect[i].name for i in df_detect.columns if True in df_detect[i]]
            # print(col_drop)
            # df_detect = df_detect.dropna(axis=1,how='all')
            # if len(col_drop) > 0:
            # df_detect = df_detect.drop(columns=[col_drop])
            col_check = df_detect.columns
            # print(col_check)
            # # print(df_detect.dtypes)
            # df_detect = df_detect.select_dtypes(include='object',exclude=['datetime64[ns, UTC]','boolean'])
            # print(df_detect.dtypes)
            # df_numeric = df_detect.select_dtypes([np.number]).columns
            # # df_numeric = df_detect.select_dtypes(include ='number').columns
            # col_date = [df_detect[i].name for i in df_detect.columns if 'DATE' in df_detect[i].name or 'FIVETRAN' in df_detect[i].name]
            # df_detect = df_detect.drop(columns=[col_date[0]])
            # col_ID = [df_detect[i].name for i in df_detect.columns if df_detect[i].name not in df_numeric]
            # for col in cols_itarate:
            for col in col_check:
                # if col in col_check:
                # if col not in col_date:
                # if df_detect[col].dtype == 'object':
                    try:
                        df = pd.read_sql(f"SELECT {col} FROM {f} WHERE {col} = '{valuesearch}'", con=ctx)
                        main_df = pd.concat([df,main_df])
                    except Exception as e:
                        continue
                        # df = pd.read_sql(f"SELECT {text_col_id} FROM {f} WHERE {text_col_id} = '{value}'", con=ctx)
                # main_df = pd.concat([df,main_df])
                # except Exception as e:
            #     print('tayyar')
            #     continue

            # df = pd.concat([df_1, df_2], ignore_index=True)
            # main_df = main_df.select_dtypes(include='object',exclude='bool')
            # main_df = main_df.replace({None : pd.NA})
            # main_df = main_df.replace({True : pd.NA})
            # main_df = main_df.replace({False : pd.NA})
            # df = df.loc[:, ~(df == 'True').any()]
            # df = df.loc[:, ~(df == 'False').any()]
            # df = df.drop(columns=df.columns[(df == 'TRUE').any()]).drop(columns=df.columns[(df == 'FALSE').any()])
            # df = df.dropna(axis=1,how='all')
            # print(df)
            # date_cols = df.select_dtypes(include=['datetime64[ns]','datetime64[ns, UTC]']).columns
            # # print(date_cols)
            # bool_cols = df.select_dtypes(include=['bool']).columns
            # for column in df.columns:
            #     df[column] = df[column].astype('str')
            # print(main_df)
            if main_df.size != 0:
                dataframes_dict[f] = main_df
            # for column in date_cols:
            #     df[column] = pd.to_datetime(df[column]).dt.date
                # if df_detect[column][0] != '0000-00-00':
                #     break
                # df_detect[column] = df_detect[column].astype('date')
        #     date_filter = df_detect['_FIVETRAN_SYNCED'][0]
            # print(df)
        #     df = pd.read_sql(f'SELECT * FROM {f} WHERE _FIVETRAN_SYNCED = "{date_filter}"',con=ctx)
        # print(df)
        # Getting all dataframes from another method of class

        # dataframes_dict = self.multiple_dataset_apply_mysql(self.host, self.user, self.password, self.database)
        # dataframes_dict = self.multiple_dataset_apply_profiling()
        # # Containing columns operation
        # for key, value in dataframes_dict.items():
        #     for key_col, columnData in value.items():
        #         for key_1, value_1 in dataframes_dict.items():
        #             for key_col_1, colondata in value_1.items():
        #                 # and "id" not in key_col and "Id" not in key_col_1
        #                 # print(key_col,key_col_1,key,key_1)
        #                 if (columnData.isin(colondata).all() == True) and (key != key_1):
        #                     d = {"table_1": [key], "col_1": [
        #                         key_col], "table_2": [key_1], "col_2": [key_col_1], "sample" : [[columnData][0][0],[colondata][0][0]]}
        #                     # print(d)
        #                     # df_output = pd.DataFrame(data=d)
        #                     frames.append(d)
                            # global df_ultimate
                            # df_ultimate = pd.concat(frames)
                        # else:
                        #     print("no match")
                            # df_ultimate = pd.DataFrame()
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
    

    def find_value_snowflake(self, col_name:str, value = 'no_val', view_or_table = 'TABLES'):
        ''' This function reads multiple tables from connected database 
        then find values searched in database tables '''
        # use glob to get all sql tables
        # in the folder
        # Creating connection variable
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
        # cursor.execute(f"SHOW {view_or_table} LIKE '{date_2}%'")
        cursor.execute(f"SHOW {view_or_table}")
        myresult = cursor.fetchall()
        # print(myresult)
        # saving all tables of database into a list
        read_tbl = [table_name[1] for table_name in myresult]
        
        # Creating an empty list to fill pandas dataframe in further steps
        global frames
        global k
        frames = []
        # frames_dict = {}
        # Loop on all tables
        for f in read_tbl:
            if value != 'no_val':
                # Read the sql tables by condition of date-time column hold
                df_detect = pd.read_sql(f'SELECT * FROM {f} LIMIT 1',con=ctx)
                df_count_rows = pd.read_sql(f'SELECT COUNT(*) AS count_rows  FROM {f} ',con=ctx)
                df_count_rows = df_count_rows['COUNT_ROWS'][0]
                # Getting list of date-time columns from detection dfs
                col_ID = [df_detect[i].name for i in df_detect.columns if f'{col_name}' == df_detect[i].name]
                delimiter = ","
                text_col_id = delimiter.join(col_ID)
                if len(col_ID) > 0:
                    df = pd.read_sql(f"SELECT {text_col_id} FROM {f} WHERE {text_col_id} = '{value}'", con=ctx)
                    count_val = df[text_col_id].count()
                    for k,v in df.items():
                        d = {"column" : [k], "table" : [f], "value" : [value] ,'occurence' : [count_val], 'table_rows' : [df_count_rows]}
                        frames.append(d)
            elif value == 'no_val':
                frames_dict = {}
                # print('davar')
                # Read the sql tables by condition of date-time column hold
                df_detect = pd.read_sql(f'SELECT * FROM {f} LIMIT 1',con=ctx)
                df_count_rows = pd.read_sql(f'SELECT COUNT(*) AS count_rows  FROM {f} ',con=ctx)
                df_count_rows = df_count_rows['COUNT_ROWS'][0]
                # Getting list of date-time columns from detection dfs
                col_ID = [df_detect[i].name for i in df_detect.columns if f'{col_name}' == df_detect[i].name]
                # col_ID = [df_detect[i].name for i in df_detect.columns if f'{col_name}' in df_detect[i].name]
                delimiter = ","
                text_col_id = delimiter.join(col_ID)
                if len(col_ID) > 0:
                    df = pd.read_sql(f"SELECT {text_col_id} FROM {f} LIMIT 1000", con=ctx)
                    df = df.drop_duplicates()
                    df_value = df.value_counts()
                    # count_val = df[text_col_id].count()s
                    # for k,v in df.items():s
                    #     d = {"column" : [k], "table" : [f], "value" : [value] ,'occurence' : [count_val], 'table_rows' : [df_count_rows]}
                    # frames = df
                    frames_dict['table_name'] = f
                    frames_dict['col_name'] = text_col_id
                    frames_dict['data'] = df_value
                    frames.append(frames_dict)

        # Changing WD for save output 
        dir = os.path.join("C:\\", "Users/cihan.oner/GitRepos/snowflake_vs_code/power_bi_reporting_solution/snwflke_profiling_res", f'{date.today()}_{value}_search')
        if not os.path.exists(dir):
            os.mkdir(dir)
        os.chdir(dir)
        # Saving containing cols dataframe 
        try:
            writer = pd.ExcelWriter(f"find_value_{value}_{k}_{date.today()}.xlsx", engine="xlsxwriter")
            df_output = pd.DataFrame(frames)
            df_output.to_excel(writer,sheet_name="found_value")
            writer.close()
            return "value find table saved"
        except NameError:
            writer = pd.ExcelWriter(f"find_value_df_write_{col_name}_{date.today()}.xlsx", engine="xlsxwriter")
            df_output = pd.DataFrame(frames)
            df_output.to_excel(writer,sheet_name="found_value")
            writer.close()
            return 'column not found'
        

    def containing_columns_mult_dfs(self, tbl_names:list, value = 'no_val', view_or_table = 'TABLES'):
        ''' This function reads multiple tables from connected database 
        then find common column names from searched tables '''
        # use glob to get all sql tables
        # in the folder
        # Creating connection variable
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
        read_tbl = [table_name[1] for table_name in myresult if table_name[1] in tbl_names]
        # # Creating an empty list to fill pandas dataframe in further steps
        # global frames
        # global k
        frames = []
        dataframes_dict = {}
        count_dict = {}
        # Loop on all tables
        if len(read_tbl) > 0:
            for f in read_tbl:
                df_detect = pd.read_sql(f'SELECT * FROM {f} LIMIT 1',con=ctx)
                df_count_rows = pd.read_sql(f'SELECT COUNT(*) AS count_rows  FROM {f} ',con=ctx)
                df_count_rows = df_count_rows['COUNT_ROWS'][0]
                # Getting list of date-time columns from detection dfs
                # df = pd.read_sql(f"SELECT {text_col_id} FROM {f} WHERE {text_col_id} = '{value}'", con=ctx)
                # count_val = df[text_col_id].count()
                dataframes_dict[f] = df_detect.columns
            for k,value in dataframes_dict.items():
                # print(k,v)
                for i in value:
                    # print(k,i)
                    if i in count_dict:
                        count_dict[i] += 1
                    else:
                        count_dict[i] = 1
                    # count_dict[i] += 1
            # count_dict_key = {k for k,v in count_dict.items() if v == len(tbl_names)}
            # count_dict_value = {v for k,v in count_dict.items() if v == len(tbl_names)}
            # print(count_dict)
            count_dict_key = {k:v for k,v in count_dict.items() if v == len(tbl_names)}
            for k,v  in count_dict_key.items():
                d = {"table" : [tbl_names], 'col_name' : k, 'qty' : v}
                frames.append(d)
        # print(frames)
        # Changing WD for save output 
        dir = os.path.join("C:\\", "Users/cihan.oner/GitRepos/snowflake_vs_code/power_bi_reporting_solution/snwflke_profiling_res", f'{date.today()}_{tbl_names[0]}_search')
        if not os.path.exists(dir):
            os.mkdir(dir)
        os.chdir(dir)
        # Saving containing cols dataframe 
        try:
            writer = pd.ExcelWriter(f"common_cols_{tbl_names[0]}_{date.today()}.xlsx", engine="xlsxwriter")
            df_output = pd.DataFrame(frames)
            df_output.to_excel(writer,sheet_name="common_cols")
            writer.close()
            return "value find table saved"
        except NameError:
            writer = pd.ExcelWriter(f"common_cols_{date.today()}.xlsx", engine="xlsxwriter")
            df_output = pd.DataFrame(frames)
            df_output.to_excel(writer,sheet_name="common_cols")
            writer.close()
            return 'column not found'
        



 
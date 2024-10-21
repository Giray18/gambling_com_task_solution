import re
import sys
import os
from datetime import datetime, timedelta, date
sys.path.insert(0, 'C:/Users/cihan.oner/GitRepos/snowflake_vs_code/power_bi_reporting_solution/src')
import time
import json
from collections import defaultdict
import pandas as pd
import numpy as np
import warnings 
warnings.filterwarnings('ignore')

class sql_text_profiler:
    '''A text profiler for shared sql queries'''

    def __init__(self,file_path):
        self.file_path = file_path


    def read_text_file(self):
        df_cols = pd.DataFrame()
        df_join_tbls = pd.DataFrame()
        df_join_cond = pd.DataFrame()
        # Creating working directory for daily partitioning
        dir = os.path.join(
            "C:\\", "Users/cihan.oner/GitRepos/snowflake_vs_code/power_bi_reporting_solution/snwflke_profiling_res", f'{date.today()}_sql_profiling')
        if not os.path.exists(dir):
            os.mkdir(dir)
        os.chdir(dir)
        with open(f'{self.file_path}', 'r') as file:
            content = file.read().splitlines()
            writer = pd.ExcelWriter(f"sql_query_{date.today()}.xlsx", engine="xlsxwriter")
            for i in content:
                i = i.replace("."," ").replace(","," ").replace("as"," ")\
                    .replace("SELECT"," ").split()
                if ((len(i) != 0) and (any(x in i for x in ["FROM",'WHERE','ORDER','']) == False)):
                    temp = pd.DataFrame([i],columns=["Alias_sql","Col_Name","GL_col_name"])
                    df_cols = pd.concat([df_cols,temp])
            df_cols.to_excel(writer, sheet_name = 'df_cols')
            for i in content:
                counter = 0
                if any(x in i for x in ["FROM"]) == True:
                    i = i.replace('('," ").replace(')'," ")\
                        .replace('FROM'," ").replace('RIGHT'," ")\
                        .replace('LEFT'," ").replace('ON'," ")\
                        .replace('JOIN'," ").split()
                    i = [x for x in i if x.__contains__("=") == False]
                    for a in range(len(i)):
                        list_appnd = []
                        if counter == len(i):
                            break
                        else:
                            list_appnd.append([i[counter]])
                            list_appnd.extend([i[counter+1]])
                            temp_2 = pd.DataFrame([list_appnd],columns=["Join_tbl","Alias_tbl"])
                            counter  += 2
                            df_join_tbls = pd.concat([df_join_tbls,temp_2])
            df_join_tbls.to_excel(writer, sheet_name = 'df_join_tbls')
            # JOIN Conditions
            for i in content:
                counter_2 = 0
                if any(x in i for x in ["FROM"]) == True:
                    i = i.replace('('," ").replace(')'," ")\
                        .replace('FROM'," ").split()
                    i = [x for x in i if x.__contains__("=") == True or x.__contains__("RIGHT") == True or x.__contains__("LEFT") == True]
                    for a in range(len(i)):
                        if counter_2+2 >= len(i):
                            list_appnd_2 = []
                            list_appnd_2.append(i[counter_2])
                            list_appnd_2.extend([i[counter_2+1]])
                            list_appnd_2.extend(["NULL"])
                            temp_3 = pd.DataFrame([list_appnd_2],columns=["Join_type","Keys","Keys-2"])
                            counter_2 += 2
                            df_join_cond = pd.concat([df_join_cond,temp_3])
                            break
                        elif ((i[counter_2].__contains__("=") == False) and (i[counter_2+1].__contains__("=") == False)):
                            list_appnd_2 = []
                            list_appnd_2.append(i[counter_2])
                            list_appnd_2.extend(["NESTED"])
                            list_appnd_2.extend(["NULL"])
                            temp_4 = pd.DataFrame([list_appnd_2],columns=["Join_type","Keys","Keys-2"])
                            counter_2 += 1
                            df_join_cond = pd.concat([df_join_cond,temp_4])
                        elif ((i[counter_2].__contains__("=") == False) and (i[counter_2+1].__contains__("=") == True) and (i[counter_2+2].__contains__("=") == True)):
                            list_appnd_2 = []
                            list_appnd_2.append(i[counter_2])
                            list_appnd_2.extend([i[counter_2+1]])
                            list_appnd_2.extend([i[counter_2+2]])
                            temp_5 = pd.DataFrame([list_appnd_2],columns=["Join_type","Keys","Keys-2"])
                            counter_2 += 3
                            df_join_cond = pd.concat([df_join_cond,temp_5])
                        elif ((i[counter_2].__contains__("=") == False) and (i[counter_2+1].__contains__("=") == True)):
                            list_appnd_2 = []
                            list_appnd_2.append(i[counter_2])
                            list_appnd_2.extend([i[counter_2+1]])
                            list_appnd_2.extend(["NULL"])
                            temp_6 = pd.DataFrame([list_appnd_2],columns=["Join_type","Keys","Keys-2"])
                            counter_2 += 2
                            df_join_cond = pd.concat([df_join_cond,temp_6])
            df_join_cond.to_excel(writer, sheet_name = 'df_join_cond')
            writer.close()
        return 'profile file saved to wd'

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector.pandas_tools import pd_writer
from sqlalchemy import create_engine
from sqlalchemy.dialects import registry
import numpy as np
import time
import datetime
import os
import sys
sys.path.insert(0, 'C:/Users/Lenovo/Desktop/gambling/gambling_com_task_solution/src')
registry.register('snowflake', 'snowflake.sqlalchemy','dialect')

class pandas_analysis_class:
    '''A python class that has methods for data profiling and manipulation'''

    def __init__(self, input_dataset_location:str) -> None:
        self.input_dataset_location = input_dataset_location

    def read_dataframe(self) -> pd.DataFrame:
        '''Loads data into a pandas dataframe from defined input path'''
        dataframe = pd.read_csv(self.input_dataset_location,index_col=False)
        return dataframe


    def pandas_data_profiler(self,output_location:str) -> pd.DataFrame:
        '''Creates xlsx formatteed file on defined location holding data profiling results 
        of requested dataframe, profiling based on checking null records, unique attiributes (columns)
        any duplicate transactions on dataframe and describing dataframe (all columns)'''
        dataframe = self.read_dataframe()
        # Empty dataframe to cumulate profiling results
        dataframes_dict = {}
        # Describe column
        describe_all_cols = dataframe.describe(include='all')
        dataframes_dict['describe_all_cols'] = describe_all_cols
        # Unique value holding attiributes (columns)
        unique_val_holding_cols = [column_name for column_name in dataframe.columns if len(dataframe[column_name]) == len(pd.unique(dataframe[column_name]))]
        df_unique = pd.DataFrame(unique_val_holding_cols,columns = ["unique_val_holding_cols"])
        dataframes_dict['unique_val_holding_cols'] = df_unique
        # Duplicate count on dataframe
        df_duplicate = dataframe.groupby(dataframe.columns.tolist(),as_index=False).size()
        df_duplicate = df_duplicate[df_duplicate['size'] > 1]
        dataframes_dict['duplicate_rows'] = df_duplicate
        # Null columns
        null_val_holding_cols = [dataframe[column] for column in dataframe.columns if dataframe[column].isna().all()]
        try:
            df_null = pd.DataFrame([null_val_holding_cols],columns = dataframe.columns)
            dataframes_dict['null_val_holding_cols'] = df_null
        except Exception as e:
            dataframes_dict['null_val_holding_cols'] = pd.DataFrame()
        # Creating working directory for daily partitioning of output files
        dir = os.path.join("C:\\", f"{output_location}", f'{datetime.date.today()}_profiled_dataframe')
        if not os.path.exists(dir):
            os.mkdir(dir)
        os.chdir(dir)
        # Writing all dataframes to xlsx file 
        writer = pd.ExcelWriter(f"profiled_dataframe_{datetime.date.today()}.xlsx", engine="xlsxwriter")
        for dataframe_name,dataframe in dataframes_dict.items():
            dataframe.to_excel(writer, sheet_name = dataframe_name)
        writer.close()
        return "data_profiling_results_saved_into_output_location"
    
    def data_manipulator(self, drop_values_rows:str, update_values:dict, date_filter = 'no_value') -> pd.DataFrame:
        '''This method gets pandas dataframe drop rows based on defined param,
        update values based on defined values, returns defined view from passed dataframe'''
        dataframe = self.read_dataframe()
        # Date_filtering for Part 3
        if date_filter != 'no_value':
            dataframe = dataframe[dataframe['Date'] <= date_filter]
             # Remove rows with requested value
            remove_val_holding_cols = [dataframe[column].name for column in dataframe.columns if dataframe[column].isin([drop_values_rows]).any()]
            for col in remove_val_holding_cols:
                dataframe = dataframe[dataframe[col] != drop_values_rows]
            # Requested view
            dataframe['HomeTeamGoals'] = dataframe['HomeTeamGoals'].astype('Int64')
            dataframe['AwayTeamScores'] = dataframe['AwayTeamScores'].astype('Int64')
            dataframe['Difference'] = dataframe.apply(lambda row: row['HomeTeamGoals'] - row['AwayTeamScores'], axis=1)
            dataframe['Home_team_Conceded'] = dataframe.apply(lambda row: row['AwayTeamScores'], axis = 1)
            dataframe['Away_team_Conceded'] = dataframe.apply(lambda row: row['HomeTeamGoals'], axis = 1)
            conditions = [(dataframe['Difference'] < 0),
                        (dataframe['Difference'] == 0),
                        (dataframe['Difference'] > 0)]
            points_home_team = [0,1,3]
            points_away_team = [3,1,0]
            dataframe['HomePoints'] = np.select(conditions,points_home_team)
            dataframe['AwayPoints'] = np.select(conditions,points_away_team)
            dataframe['Home_team_Win'] = dataframe.apply(lambda row: row['HomePoints'] == 3, axis = 1)
            dataframe['Away_team_Win'] = dataframe.apply(lambda row: row['AwayPoints'] == 3, axis = 1)
            dataframe['Draw_a'] = dataframe.apply(lambda row: row['AwayPoints'] == 1, axis = 1)
            dataframe['Draw_h'] = dataframe.apply(lambda row: row['HomePoints'] == 1, axis = 1)
            dataframe['Home_team_Loss'] = dataframe.apply(lambda row: row['HomePoints'] == 0, axis = 1)
            dataframe['Away_team_Loss'] = dataframe.apply(lambda row: row['AwayPoints'] == 0, axis = 1)
            chart_1 = dataframe.groupby(["HomeTeam"]).agg({"HomePoints": ["sum"], "Home_team_Loss": ["sum"], 'Draw_h' : ["sum"], "Home_team_Win": ["sum"], "HomeTeamGoals": ["sum"],"Home_team_Conceded": ["sum"], "HomeTeam": ["count"]}) 
            chart_2 = dataframe.groupby(["AwayTeam"]).agg({"AwayPoints": ["sum"], "Away_team_Loss": ["sum"], 'Draw_a' : ["sum"], "Away_team_Win": ["sum"], "AwayTeamScores": ["sum"],"Away_team_Conceded":["sum"], "AwayTeam": ["count"]})
            view = pd.concat([chart_1, chart_2], axis=1)
            view['POINTS'] = view['HomePoints']['sum'] + view['AwayPoints']['sum']
            view['GOALS_FOR'] = view['HomeTeamGoals']['sum'] + view['AwayTeamScores']['sum']
            view['MATCHES_PLAYED'] = view['HomeTeam']['count'] + view['AwayTeam']['count']
            view['WIN'] = view['Home_team_Win']['sum'] + view['Away_team_Win']['sum']
            view['DRAW'] = view['Draw_h']['sum'] + view['Draw_a']['sum']
            view['LOSS'] = view['Home_team_Loss']['sum'] + view['Away_team_Loss']['sum']
            view['GOALS_AGAINST'] = view['Home_team_Conceded']['sum'] + view['Away_team_Conceded']['sum']
            view['GOAL_DIFFERENCE'] = view['GOALS_FOR'] - view['GOALS_AGAINST']
            view = view[['POINTS','GOALS_FOR', "MATCHES_PLAYED",'WIN','DRAW','LOSS','GOALS_AGAINST','GOAL_DIFFERENCE']]
            view = view.sort_values(by=['POINTS'], ascending = False)
            # Rename Club name 'Brighton'
            view = view.rename(update_values, axis=0)
            # Adding info columns to view
            view['POSITION'] = view['POINTS'].rank(method='max',ascending = False).astype('Int64')
            view = view.rename_axis('CLUB')
            view['CLUB'] = view.index
            view['primary_key'] = view['CLUB']
            view = view[['POSITION','CLUB',"MATCHES_PLAYED",'WIN','DRAW','LOSS','GOALS_FOR','GOALS_AGAINST','GOAL_DIFFERENCE','POINTS']]
            # view_ni = pd.DataFrame(view,index=None)
            # view.columns = map(lambda x: str(x).upper(), view.columns)
            # view.rename(columns=''.join,inplace=True)
            # Saving output dataframe as CSV to defined path
            view.to_csv('table_chart.csv')
            return view
        else:
            # Remove rows with requested value
            remove_val_holding_cols = [dataframe[column].name for column in dataframe.columns if dataframe[column].isin([drop_values_rows]).any()]
            for col in remove_val_holding_cols:
                dataframe = dataframe[dataframe[col] != drop_values_rows]
            # Requested view
            dataframe['HomeTeamGoals'] = dataframe['HomeTeamGoals'].astype('Int64')
            dataframe['AwayTeamScores'] = dataframe['AwayTeamScores'].astype('Int64')
            dataframe['Difference'] = dataframe.apply(lambda row: row['HomeTeamGoals'] - row['AwayTeamScores'], axis=1)
            dataframe['Home_team_Conceded'] = dataframe.apply(lambda row: row['AwayTeamScores'], axis = 1)
            dataframe['Away_team_Conceded'] = dataframe.apply(lambda row: row['HomeTeamGoals'], axis = 1)
            # print(dataframe)
            conditions = [(dataframe['Difference'] < 0),
                        (dataframe['Difference'] == 0),
                        (dataframe['Difference'] > 0)]
            points_home_team = [0,1,3]
            points_away_team = [3,1,0]
            dataframe['HomePoints'] = np.select(conditions,points_home_team)
            dataframe['AwayPoints'] = np.select(conditions,points_away_team)
            dataframe['Home_team_Win'] = dataframe.apply(lambda row: row['HomePoints'] == 3, axis = 1)
            dataframe['Away_team_Win'] = dataframe.apply(lambda row: row['AwayPoints'] == 3, axis = 1)
            dataframe['Draw_a'] = dataframe.apply(lambda row: row['AwayPoints'] == 1, axis = 1)
            dataframe['Draw_h'] = dataframe.apply(lambda row: row['HomePoints'] == 1, axis = 1)
            dataframe['Home_team_Loss'] = dataframe.apply(lambda row: row['HomePoints'] == 0, axis = 1)
            dataframe['Away_team_Loss'] = dataframe.apply(lambda row: row['AwayPoints'] == 0, axis = 1)
            chart_1 = dataframe.groupby(["HomeTeam"]).agg({"HomePoints": ["sum"], "Home_team_Loss": ["sum"], 'Draw_h' : ["sum"], "Home_team_Win": ["sum"], "HomeTeamGoals": ["sum"],"Home_team_Conceded": ["sum"], "HomeTeam": ["count"]}) 
            chart_2 = dataframe.groupby(["AwayTeam"]).agg({"AwayPoints": ["sum"], "Away_team_Loss": ["sum"], 'Draw_a' : ["sum"], "Away_team_Win": ["sum"], "AwayTeamScores": ["sum"],"Away_team_Conceded":["sum"], "AwayTeam": ["count"]})
            view = pd.concat([chart_1, chart_2], axis=1)
            view['POINTS'] = view['HomePoints']['sum'] + view['AwayPoints']['sum']
            view['GOALS_FOR'] = view['HomeTeamGoals']['sum'] + view['AwayTeamScores']['sum']
            view['MATCHES_PLAYED'] = view['HomeTeam']['count'] + view['AwayTeam']['count']
            view['WIN'] = view['Home_team_Win']['sum'] + view['Away_team_Win']['sum']
            view['DRAW'] = view['Draw_h']['sum'] + view['Draw_a']['sum']
            view['LOSS'] = view['Home_team_Loss']['sum'] + view['Away_team_Loss']['sum']
            view['GOALS_AGAINST'] = view['Home_team_Conceded']['sum'] + view['Away_team_Conceded']['sum']
            view['GOAL_DIFFERENCE'] = view['GOALS_FOR'] - view['GOALS_AGAINST']
            view = view[['POINTS','GOALS_FOR', "MATCHES_PLAYED",'WIN','DRAW','LOSS','GOALS_AGAINST','GOAL_DIFFERENCE']]
            view = view.sort_values(by=['POINTS'], ascending = False)
            # Rename Club name 'Brighton'
            view = view.rename(update_values, axis=0)
            # Adding info columns to view
            view['POSITION'] = view['POINTS'].rank(method='max',ascending = False).astype('Int64')
            view = view.rename_axis('CLUB')
            view['CLUB'] = view.index
            view['primary_key'] = view['CLUB']
            view = view[['POSITION','CLUB','MATCHES_PLAYED','WIN','DRAW','LOSS','GOALS_FOR','GOALS_AGAINST','GOAL_DIFFERENCE','POINTS']]
            # view.columns = map(lambda x: str(x).upper(), view.columns)
            return view


class snowflake_analysis_class(pandas_analysis_class):
    '''A python class to apply data operations on Snowflake'''

    def __init__(self, user, password, account,  database, schema) -> None:
        super().__init__(input_dataset_location = 'C:/Users/Lenovo/Desktop/gambling/gambling_com_task_solution/Python_Test/Fixtures.csv')
        self.user = user
        self.password = password
        self.account = account
        self.database = database
        self.schema = schema

    def table_operations(self, table_to_drop = 'STANDINGS_10'):
        '''Function for data operations on Snowflake'''
        # Getting tabular data from Part 2
        df_to_write = super().data_manipulator('Man City', {'Brighton' : 'Brighton & Hove Albion'})
        # Create a cursor from the connection variable
        ctx = snowflake.connector.connect(
        user = self.user,
        password = self.password,
        account = self.account,
        database = self.database,
        schema = self.schema,
        )
        cursor = ctx.cursor()

        snowflake_username = self.user
        snowflake_password = self.password
        snowflake_account = self.account
        # snowflake_warehouse = os.environ['SNOWFLAKE_WAREHOUSE']
        snowflake_database = self.database
        snowflake_schema = self.schema

        engine = create_engine(f'snowflake://{self.user}:{self.password}@{self.account}/{self.database}/{self.schema}'.format(
            user=snowflake_username,
            password=snowflake_password,
            account=snowflake_account,
            db=snowflake_database,
            schema=snowflake_schema,
            # warehouse=snowflake_warehouse,
        ))


        # Reading table names on database into read_tbl list
        cursor.execute('DELETE FROM STANDINGS_10 WHERE CLUB IN (SELECT CLUB FROM STANDINGS_10)')
        # myresult = cursor.fetchall()
        # print(myresult)
        # print(df_to_write)
        # df_source = pd.read_sql(f'SELECT * FROM {table_to_drop}', con = ctx)
        # print(df_source)
        # print(df_to_write.columns)
        # df_write = pd.DataFrame(data=df_to_write,columns=df_source.columns)
        # print(df_write)
        # source_data_df = df_to_write

        df_to_write.columns = [''.join(x) for x in df_to_write.columns]

        # print(df_to_write.columns)
        # print(df_to_write)
        # # Write dataframe to target table
        # df_to_write.to_sql('standings_10', con = engine, index = False, method=pd_writer, schema='TECH_TEST',  if_exists='append')
        # CALISAN DRIVER BU
        df_to_write.to_sql('standings_10',engine,index=False,method=pd_writer,if_exists='append')
        # df_to_write.to_sql('standings_10', ctx, index = False,method =pd_writer)
        # try:
        #     df_to_write.to_sql('STANDINGS_10', ctx, index = False,method =pd_writer)
        # except Exception as e:
        #     print('davar')
        # success, nchunks, nrows, _ = write_pandas(ctx, df_to_write,  'STANDINGS_10' ,index = False)
        # cur = ctx.cursor()
        # sql = f"select * from {table_to_drop}"
        # cur.execute(sql)
        # cur.close()
        # ctx.close()
        # df_detect = pd.read_sql(f'SELECT * FROM {f} LIMIT 100',con=ctx)



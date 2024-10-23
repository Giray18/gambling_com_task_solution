import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import numpy as np
import time
import datetime
import os
import sys

class PandasAnalysisClass:
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
            print("The error is: No null values on dataset so an empty dataframe created for  to avoid exception on excel writer// ",e)
            dataframes_dict['null_val_holding_cols'] = pd.DataFrame()
        # Writing all dataframes to xlsx file 
        writer = pd.ExcelWriter(f"profiled_dataframe_{datetime.date.today()}.xlsx", engine="xlsxwriter")
        for dataframe_name,dataframe in dataframes_dict.items():
            dataframe.to_excel(writer, sheet_name = dataframe_name)
        writer.close()
        return "data_profiling_results_saved_into_output_location"
    
    
    def data_manipulator(self, drop_values_rows:str, update_values:dict, date_filter = 'no_value') -> pd.DataFrame:
        '''This method gets pandas dataframe drop rows based on defined param,
        update values based on defined param, returns requested view from passed dataframe'''
        dataframe = self.read_dataframe()
        # Date_filtering conditionally for Part 3
        if date_filter != 'no_value':
            dataframe = dataframe[dataframe['Date'] <= date_filter]
        else:
            dataframe
        # Remove rows with requested value 'Man City'
        dataframe_row_count = dataframe.shape[0]
        remove_val_holding_cols = [dataframe[column].name for column in dataframe.columns if dataframe[column].isin([drop_values_rows]).any()]
        for col in remove_val_holding_cols:
            dataframe = dataframe[dataframe[col] != drop_values_rows]
        dataframe_row_count_after_rem = dataframe.shape[0]
        # Quality check for defined constraints
        assert dataframe_row_count_after_rem < dataframe_row_count, 'Row removal did not worked!'
        # Requested view
        dataframe['HomeTeamGoals'] = dataframe['HomeTeamGoals'].astype('Int64')
        dataframe['AwayTeamScores'] = dataframe['AwayTeamScores'].astype('Int64')
        dataframe['Difference'] = dataframe['HomeTeamGoals'] - dataframe['AwayTeamScores']
        dataframe['Home_team_Conceded'] = dataframe['AwayTeamScores']
        dataframe['Away_team_Conceded'] = dataframe['HomeTeamGoals']
        # Conditional creation of points columns
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
        # Aggregated columns by team names
        chart_1 = dataframe.groupby(["HomeTeam"]).agg({"HomePoints": ["sum"], "Home_team_Loss": ["sum"], 'Draw_h' : ["sum"], "Home_team_Win": ["sum"], "HomeTeamGoals": ["sum"],"Home_team_Conceded": ["sum"], "HomeTeam": ["count"]}) 
        chart_2 = dataframe.groupby(["AwayTeam"]).agg({"AwayPoints": ["sum"], "Away_team_Loss": ["sum"], 'Draw_a' : ["sum"], "Away_team_Win": ["sum"], "AwayTeamScores": ["sum"],"Away_team_Conceded":["sum"], "AwayTeam": ["count"]})
        # Union of dataframes to a unified view
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
        # Rename club name 'Brighton' as 'Brighton & Hove Albion'
        view = view.rename(update_values, axis=0)
        # Adding additional columns to view dataframe
        view['POSITION'] = view['POINTS'].rank(method='max',ascending = False).astype('Int64')
        view = view.rename_axis('CLUB')
        view['CLUB'] = view.index
        view['primary_key'] = view['CLUB']
        # Filtering dataframe by column names
        view = view[['POSITION','CLUB',"MATCHES_PLAYED",'WIN','DRAW','LOSS','GOALS_FOR','GOALS_AGAINST','GOAL_DIFFERENCE','POINTS']]
        # Quality check for defined constraints
        rename_counts = view['CLUB'].value_counts()['Brighton & Hove Albion']
        assert rename_counts > 0, 'Cell update did not worked!'
        return view
    
    def data_manipulator_date_filter(self,data_manipulator) -> pd.DataFrame:
        '''This method wraps function data_manipulator and saves it as csv to wd after applying passed date filter'''
        def wrapper(*args):
            # Saving output dataframe as CSV to defined path (working directory)
            data_manipulator(*args).to_csv('table_chart_filtered.csv')
            return "dataframe saved to working directory"
        return wrapper

class snowflake_analysis_class(PandasAnalysisClass):
    '''A python class to apply data operations to dataframes coming from pandas_analysis_class on Snowflake '''

    def __init__(self, user, password, account,  database, schema) -> None:
        super().__init__(input_dataset_location = 'C:/Users/Lenovo/Desktop/gambling/gambling_com_task_solution/Python_Test/Fixtures.csv')
        self.user = user
        self.password = password
        self.account = account
        self.database = database
        self.schema = schema

    def table_operations(self, table_to_drop = 'STANDINGS_10'):
        '''Function for data operations on Snowflake, delete all rows from defined table 
        then writes dataframe in defined table'''
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

        # Reading table names on database into read_tbl list
        cursor.execute(f'DELETE FROM {table_to_drop} WHERE CLUB IN (SELECT CLUB FROM {table_to_drop})')
        df_source = pd.read_sql(f'SELECT * FROM {table_to_drop}', con = ctx)
        assert len(df_source) == 0, 'Table delete did not work'

        # Writing dataframe to Snowflake`s defined table
        try:
            success, nchunks, nrows, _ = write_pandas(ctx, df_to_write,  table_to_drop ,index = False)
        except Exception as e:
            print("The error is: columns names are not matching with target table on snowflake",e)
            df_to_write.columns = [''.join(x) for x in df_to_write.columns]
            success, nchunks, nrows, _ = write_pandas(ctx, df_to_write,  table_to_drop ,index = False)
        df_source = pd.read_sql(f'SELECT * FROM {table_to_drop}', con = ctx)
        assert len(df_source) > 0, 'Table write did not work'
        ctx.close()
        return "Table wrote on Snowflake location"
# Python Task Solution
This repo contains elements used for python based task solution. Basically, tasks is consisting of data manipulation and aggregated view creation activities by using Python.

## Solution Design and Details
On solution concept OOP knowledge demostrated by creating 2 seperate python class on 1 .py file that holds class methods (functions) to provide solution for task requirements. 1 Jupyter notebook to initiate class and their methods. Mentioned class is saved into a custom module and being called into notebook as module. Steps of tasks are defined on code chunks of notebook as markdown. For to demonstrate advanced knowledge on python concepts like wrapper function, list comprehension, exception handling and assert functions also used on solution.

As addition to requirements document, For to check source data quality and getting to know dataset without opening up source file. An extra method is used which creates an output file regarding mentioned quality checks. Location of output file can be found on below part.

### Important File Locations
- (.py file for class and methods) https://github.com/Giray18/gambling_com_task_solution/blob/main/Python_Test/data_profiler/pandas_analysis_class.py
- (jupyter notebook) https://github.com/Giray18/gambling_com_task_solution/blob/main/Python_Test/task_1_sql.ipynb
- (requirements.txt) https://github.com/Giray18/gambling_com_task_solution/blob/main/requirements.txt
- (source file) https://github.com/Giray18/gambling_com_task_solution/blob/main/Python_Test/Fixtures.csv
- (requirements) https://github.com/Giray18/gambling_com_task_solution/blob/main/Python_Test/Python_Test_Brief_v1.1.docx
- (CSV file created on STEP 3) https://github.com/Giray18/gambling_com_task_solution/blob/main/Python_Test/table_chart_filtered.csv
- (Source data profiling activity output) https://github.com/Giray18/gambling_com_task_solution/blob/main/Python_Test/profiled_dataframe_2024-10-23.xlsx

### Details of Solution
 - PART 1 : Loaded source data to Pandas Dataframe by running PandasAnalysisClass`s read_dataframe function
 - PART 2 : Mentioned datacleaning and view creation works done with  PandasAnalysisClass`s data_manipulator function
 - PART 3 : Requested dataframe filtering and write to CSV works done with PandasAnalysisClass`s data_manipulator and data_manipulator_date_filter function. Here data_manipulator_date_filter is wrapper to data_manipulator function by adding write to CSV capability in wrapper function. Date filtering capability already put on wrapped function (data_manipulator) on PART 2.
 - PART 4 : Table delete and write operation for Snowflake platform done by initiating snowflake_analysis_class and its table_operations function.

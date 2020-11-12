## SQL-Python-Project

## Project Description<br/>

1. Gracefully handle the connection to the database server.<br/>
2. Get all survey data from SQL database. Impliment similer functionality in database function dbo.fn_GetAllSurveyDataSQL.<br/>
3. Implement a trigger like behaviour for creating/altering the view vw_AllSurveyData whenever applicable.<br/>
4. For achieving (3) above, a persistence component (in any format you like: CSV, XML, JSON, etc.), storing the last known surveys’ structures should be in place. It is not acceptable to just recreate the view every time: the trigger behaviour must be replicated.<br/>
5. Extract “always-fresh” pivoted survey data, in a CSV file, adequately named.<br/>
6. In terms of allowed libraries and beyond the recommended pyodbc & pandas, you are free to use anything you like, but with this mandatory requirement: your Python application should not require the user to install packages before the run.<br/>

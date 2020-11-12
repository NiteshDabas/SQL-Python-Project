#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
### Software Engineering + Data Wrangling with SQL and Python
### Submission from Nitesh Dabas
"""

############################################################
### Python packages installation code
############################################################
def install_packages():
    import subprocess
    import sys
    from os import path
    
    global pd, np, pyodbc, sys, path
    #install package pyodbc for database connection
    try:
        import pyodbc as pyodbc
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", 'pyodbc'])
    finally:
        import pyodbc as pyodbc 
    #import pandas
    try:
        import pandas as pd
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", 'pandas'])
    finally:
        import pandas as pd   
    #import numpy
    try:
        import numpy as np
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", 'numpy'])
    finally:
        import numpy as np 
        
############################################################
### Gracefully handle the connection to the database server.
############################################################
def connect_database():
    global connection
    #Connect to database
    try:
        connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                                    'SERVER=127.0.0.1,1433;'
                                    'DATABASE=Survey_Sample_A19;'
                                    'UID=SA;'
                                    'PWD=*****;')
        print("Database connection successful. \n")
        return connection 
    #Failure in connection
    except pyodbc.Error as connstate:
        connection_message=connstate.args[0]
        sys.exit("Database connection failed with error:" + connection_message)

############################################################
### Get the latest survey structure in database 
############################################################
def db_getsurvey_structure(my_connection):
    cursor_SurveyId = my_connection.cursor() #curser to fetch surveyid from Survey table
    sql_SurveyID = "SELECT SurveyId FROM Survey ORDER BY SurveyId"
    cursor_SurveyId.execute(sql_SurveyID)
    db_SurveyID = cursor_SurveyId.fetchall()    
    my_collected_data = [0,0,0] #initialization for data to be collected about Survey structure 
    try:
        for i_survey_id in db_SurveyID:
            my_survey_id = i_survey_id[0] #initilize
            sql_myQuestion = 'SELECT * FROM ( SELECT SurveyId,QuestionId,1 as InSurvey FROM SurveyStructure WHERE SurveyId ='+str(my_survey_id)
            sql_myQuestion = sql_myQuestion+' UNION SELECT '+str(my_survey_id) +' as SurveyId, q.QuestionId, 0 as InSurvey FROM Question as q'
            sql_myQuestion = sql_myQuestion+' WHERE NOT EXISTS(SELECT * FROM SurveyStructure as s WHERE s.SurveyId = '+str(my_survey_id)
            sql_myQuestion = sql_myQuestion+' AND s.QuestionId = q.QuestionId) ) as t ORDER BY QuestionId'
            cursor_myQuestion = my_connection.cursor() #cursor for survey structure query
            cursor_myQuestion.execute(sql_myQuestion)
            db_myQuestionData = cursor_myQuestion.fetchall()
            for i_RelevantSurveyId, i_RelevantQuestionID, i_RelevantSurvey in db_myQuestionData:
                my_data=np.array([i_RelevantSurveyId, i_RelevantQuestionID, i_RelevantSurvey])
                my_collected_data = np.vstack((my_collected_data,my_data))
        my_survey_structure=pd.DataFrame(my_collected_data[1:,:]).astype(int)
        my_survey_structure.columns=['SurveyId','QuestionID','SurveyedQuestion']
        return my_survey_structure
    except:
        sys.exit("Error \n")
    finally:
        cursor_SurveyId.close() #close opened cursor
        cursor_myQuestion.close() #close opened cursor
        
############################################################
### Replicate the algorithm of the dbo.fn_GetAllSurveyDataSQL stored function.
############################################################
def sql_GetAllSurveyData(i_relevant_QuestionId, i_relevant_SurveyId, i_SurveyId, str_temp, i_index):
    if(i_index == 1): #query to collect retrieve questions for each survey and response data 
        sql_survey_question_answer1 = "COALESCE((SELECT a.Answer_Value FROM Answer as a " \
                                        + "WHERE a.UserId = u.UserId AND a.SurveyId = <SURVEY_ID> " \
                                            + "AND a.QuestionId = <QUESTION_ID>), -1) AS ANS_Q<QUESTION_ID>"                                       
        sql_survey_question_answer2 = "NULL AS ANS_Q<QUESTION_ID>"
        #if question is not in this survey
        if i_relevant_SurveyId == 0: strGetAllSurveyData_P1 = sql_survey_question_answer2.replace('<QUESTION_ID>', str(i_relevant_QuestionId))
        #if question is in this survey
        else: strGetAllSurveyData_P1 = sql_survey_question_answer1.replace('<QUESTION_ID>', str(i_relevant_QuestionId))
        return(strGetAllSurveyData_P1)
    elif (i_index == 2): #Outer SQL for the current Survey, from the template
        sql_survey_query = " SELECT UserId, <SURVEY_ID> as SurveyId, <DYNAMIC_QUESTION_ANSWERS> FROM [User] as u " \
                                + "WHERE EXISTS (SELECT * FROM Answer as a WHERE u.UserId = a.UserId AND a.SurveyId = <SURVEY_ID>)"
        strGetAllSurveyData = '' #initialize
        strGetAllSurveyData = sql_survey_query.replace('<DYNAMIC_QUESTION_ANSWERS>', str(str_temp))
        strGetAllSurveyData = strGetAllSurveyData.replace('<SURVEY_ID>', str(i_SurveyId))
        return str(strGetAllSurveyData)

def db_get_latest_surveydata(db_latest_survey_structure):
    l_QuestionId_list = db_latest_survey_structure['QuestionID'].unique() #get unique question-ids
    l_SurveyId_list = db_latest_survey_structure['SurveyId'].unique() #get unique survey-ids
    i_max_question_id=np.max(l_QuestionId_list) #max to identify last question id 
    i_max_survey_id=np.max(l_SurveyId_list) #max to identify last surveyid 
    str_fetch_survey_data = "" #initialize
    for i_survey_id in l_SurveyId_list: #iterate for all survey-ids
        str_temp="" #initialize
        for i_question_id in l_QuestionId_list:  #iterate for all question-ids
            i_RelevantSurvey_id = db_latest_survey_structure[(db_latest_survey_structure['SurveyId'] == i_survey_id) 
                                                & (db_latest_survey_structure['QuestionID'] == i_question_id)].iloc[:,-1].values
            str_temp=str_temp+sql_GetAllSurveyData(i_question_id,i_RelevantSurvey_id, 0, "", 1)
            if i_question_id<i_max_question_id: str_temp+="," # for comma seperated question-ids
            else: str_temp = str_temp
        str_fetch_survey_data += sql_GetAllSurveyData(0,0,i_survey_id, str_temp, 2)
        if i_survey_id<i_max_survey_id: str_fetch_survey_data+="UNION" #Union queries for different survey-ids
        else: str_fetch_survey_data = str_fetch_survey_data
    return str_fetch_survey_data #return quesry to fetch latest data
        
############################################################
### Compare local surevey structure with database surevy structure - dbo.trg_refreshSurveyView replication
############################################################
def check_local_survey_structure(my_connection):
    i_flag = 0 #flag to indicate change in survey data file and hence corresponding return from function
    db_survey_structure = db_getsurvey_structure(my_connection)
    if path.exists("./Latest_Survey_Structure.csv"):
        print("Survey Structure file already exists. \n")
        print("Now we need to check if file has changed? \n")
        local_survey_structure = pd.read_csv("./Latest_Survey_Structure.csv", sep=",", index_col=0)  
        if(db_survey_structure.equals(local_survey_structure)):
            print("Survey Structure file has not changed. \n")
        else:
            i_flag = 1 #file has changed
            print("Survey Structure file has changed. \n")
            print("We need to get the latest changes in db survey updated in local survey file. \n")
            sql_latest_surveydata = db_get_latest_surveydata(db_survey_structure) #since file as changed, make query to fetch latest db surveydata
            db_survey_structure.to_csv("./Latest_Survey_Structure.csv", sep=",") #save survey structure db data in csv
            print("Survey structure file - Latest_Survey_Structure.csv updated in current directory. \n")
    else: #file doesnt exist - first time
        i_flag = 1 #file has changed
        sql_latest_surveydata = db_get_latest_surveydata(db_survey_structure) #since file as changed, make query to fetch latest db surveydata
        db_survey_structure.to_csv("./Latest_Survey_Structure.csv", sep=",") #save survey structure db data in csv
        print("Survey structure file - Latest_Survey_Structure.csv created in current directory. \n")
    #return sql query only if data as changed; otherwise keep already existing data as its same in db
    if(i_flag): return sql_latest_surveydata
    else: return 0
        
############################################################
### Main Function
############################################################
def main():
    #install packages required for this project dynamically
    install_packages()
    #connect to the database
    my_connection = connect_database()
    #Check whether surveys’ structures is in place and imitate trigger dbo.trg_refreshSurveyView
    surveydata = check_local_survey_structure(my_connection)
    if surveydata:
        data=pd.read_sql(surveydata, my_connection)
        data.fillna(value=pd.np.nan, inplace=True)
        data.to_csv("./AllSurveyData.csv")
        print("Survey data file - AllSurveyData.csv created/updated in current directory. \n")
    else:
        print("No update in existing file. Survey data is up to date. \n")
    my_connection.close() #close db connection
    del my_connection #delete db connection
         
if __name__ == "__main__":
    #call to main function    
    main()

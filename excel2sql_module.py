#쿼리 한번에 작성 후 execute

#라이브러리 호출

import psycopg2
import time

#필요한 값 입력 받기
host = input('input "Host"')
dbname = input('input "DBname"')
user = input('input "User"')
password = input('input "Password"')
port = input('input "Port"')
table_name = input('input "Table name"')

#pgadmin과 연동
try:
    conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=port)
    print("connected to postgreSQL")
    print("-----------------")
    cur = conn.cursor()
    

except:
    print("not connected")
    print("-----------------")
    
########################################################################################################################
########################################################################################################################   


#엑셀파일 선택(xlsx, xls, csv)
import os
import pandas as pd
import numpy as np
from tkinter import filedialog
from tkinter import messagebox

#files 변수에 선택 파일 경로 넣기
files = filedialog.askopenfilenames(initialdir="/",\
                 title = "파일을 선택 해 주세요",\
                    filetypes = (("*.xlsx","*xlsx"),("*.xls","*xls"),("*.csv","*csv")))



#파일 선택 안했을 때 메세지 출력
if files == '':
    messagebox.showwarning("경고", "파일을 추가 하세요")    

    
files = list(files)
fail_list = files[:]
success_list = []
print("file list: ") 
print(files)    #files 리스트 값 출력
print("-----------------")




    
########################################################################################################################
########################################################################################################################
start = time.time() #시간 측정 시작
    
#dir_path에 파일경로 하나씩 넣어서 읽기


for dir_path in files:                                             
    df = pd.read_excel(dir_path)   
    df_numpy = df.to_numpy()
    print(dir_path.split("/")[-1], "file is running....")
    
    #SQL문 실행    
    
    sqlString_head = "INSERT INTO " + table_name + " VALUES "
    sqlString_tail = ""
    
    file_name = dir_path.split("/")[-1]
    

    
    for idx in range(len(df)):
        sqlString_tail += "(%s, %s, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s, '%s'), " %tuple(df_numpy[idx])
        
    sqlString_tail = sqlString_tail[:-2]
    sqlString = sqlString_head + sqlString_tail
    
    try:
        cur = conn.cursor()
        cur.execute(sqlString)
        conn.commit()
        print(file_name, "file's Commit is done:") 
        print("Duration of time: ", time.time() - start, "Seconds")#시간 측정 종료
        print("-----------------")
        
        fail_list.remove(dir_path)
        success_list.append(dir_path)

    except:
        print(file_name, "file cannot Execute in SQL")
        print("-----------------")
        conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=port) #재접속
        
        


# print("SQL commit is done")
# print("-----------------")
conn.close()
print("SQL connection ended")
print("-----------------")
print("Success File List:")
print(success_list)
print("-----------------")
print("Fail File List:")
print(fail_list)
print("-----------------")
print("Total Duration of time: ", time.time() - start, "Seconds")#시간 측정 종료

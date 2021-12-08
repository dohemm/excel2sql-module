#쿼리 하나씩 execute

#라이브러리 호출

import psycopg2
import time
import pandas as pd
import numpy as np

#필요한 값 입력 받기
host = input('input "Host": ')
dbname = input('input "DBname": ')
user = input('input "User": ')
password = input('input "Password": ')
port = input('input "Port": ')

#pgadmin과 연동
server_suc = 0
file_suc = 0

try:
    conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=port)
    print("connected to postgreSQL")
    print("-----------------")
    server_suc = 1
    #엑셀파일 선택(xlsx, xls, csv)
    
except:
    
    
    print("서버 접속 오류(입력값을 다시 확인하세요)")
    conn.close()
    print("postgreSQL과의 연결이 종료되었습니다.")
    
if server_suc== 1:
    
    filename = input("같은 폴더 내에 있는 파일명을 입력하세요(확장자명까지): ")
    print("-----------------")

    try:
        df = pd.read_excel(filename) 
    except: 
        pass

    try:
        df = pd.read_csv(filename, encoding='cp949') 
    except: 
        pass
    
    try:
        df_numpy = df.to_numpy()      #넘파이 행렬로 변환하여 속도 향상
        del df #안 쓰는 데이터 프레임 삭제하여 메모리 확보
        file_suc = 1
        print("table shape: ", df_numpy.shape)
        print("파일이 정상적으로 로드되었습니다. ")
    
    except:
        print("파일 오류(확장자, 파일 존재유무 확인)")
        conn.close()
        print("-----------------")
        print("Program end")
        
        
if file_suc == 1:

    table_name = input('input "Table name": ')
    
    print("------------------------------")
    print("Host: ", host)
    print("DBname: ", dbname)
    print("User: ", user)
    print("Table: ", table_name)
    print("------------------------------")
    print("위의 테이블로", filename, "데이터를 전송합니다. 맞습니까?")
    
    check = input("y/n로 입력")
    
    if check == "y" :
        start = time.time() #시간 측정 시작


        #SQL문 실행    

        sqlString = "INSERT INTO " + table_name + " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"

        fail_row = []
        fail_company_id = []

        cur = conn.cursor()

        for idx in range(len(df_numpy)):

            try:
                cur.execute(sqlString, tuple(df_numpy[idx]))



            except :

                fail_row.append(idx)
                fail_company_id.append(df_numpy[idx][0])

                conn.rollback()
                break

        if len(fail_row) ==0 :
            conn.commit()
            print("------------------------------")

            print("Commit is done.")

            print("Total Duration of time: ", time.time() - start, "Seconds")#시간 측정 종료



        else:
            print("------------------------------")

            print("[오류발견]commit을 진행하지 않습니다!")
            print("오류 행(0부터 시작): ", fail_row)
            print("오류 회사 코드: ", fail_company_id)





        # print("SQL commit is done")
        # print("-----------------")
        conn.close()
        print("postgreSQL과의 연결이 종료되었습니다.")#시간 측정 종료
    
    elif check == "n" :
        conn.close()
        print("------------------------------")

        print("postgreSQL과의 연결이 종료되었습니다.")
        
    else:
        conn.close()
        print("y, n 이외의 문자가 입력되었습니다.")
        print("postgreSQL과의 연결이 종료되었습니다.")









    ########################################################################################################################
    ########################################################################################################################









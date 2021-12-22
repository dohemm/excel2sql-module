# 쿼리 한나씩 execute

# 라이브러리 호출
# import openpyxl        #  openpyxl 라이브러리 필요시 설치 및 import!
import psycopg2
import time
import pandas as pd
import numpy as np

# 필요한 값 입력 받기
host = input('input "Host": ')
dbname = input('input "DBname": ')
user = input('input "User": ')
password = input('input "Password": ')
port = input('input "Port": ')

# postgresql과 연동
server_suc = 0
file_suc = 0

try:

    #postgresql db에 연결

    conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=port)
    print("connected to postgreSQL")
    print("-----------------")
    server_suc = 1

except:

    #postgresql db에 연결 실패 시

    print("서버 접속 오류(입력값을 다시 확인하세요)")
    print("postgreSQL과의 연결이 종료되었습니다.")


#db 연결 성공 시
if server_suc == 1:

    filename = input("같은 폴더 내에 있는 파일명을 입력하세요(확장자명까지): ")
    print("-----------------")

    # 엑셀파일 선택(xlsx, xls, csv, 등..)
    try:
        df = pd.read_excel(filename)
        df = df.fillna('null')
    except:
        pass

    try:
        df = pd.read_csv(filename, encoding='cp949')
        df = df.fillna('null')
    except:
        pass

    try:
        df_numpy = df.to_numpy()  # 넘파이 행렬로 변환하여 속도 향상
        del df  # 안 쓰는 데이터 프레임 삭제하여 메모리 확보
        file_suc = 1
        print("table shape: ", df_numpy.shape)
        print("파일이 정상적으로 로드되었습니다. ")

    except:
        #넘파이 행렬로 변환이 안 되면, 데이터프레임이 제대로 로드된 게 아니므로 종료.

        print("파일 오류(확장자, 파일 존재유무 확인)")
        conn.close()
        print("-----------------")
        print("Program end")

#데이터프레임 로드 및 넘파이 행렬로의 변환 성공 시
if file_suc == 1:


    #테이블 정보 입력
    table_name = input('input "Table name": ')

    #전송 전, 이상유무 확인
    print("------------------------------")
    print("Host: ", host)
    print("DBname: ", dbname)
    print("User: ", user)
    print("Table: ", table_name)
    print("------------------------------")
    print("위의 테이블로", filename, "데이터를 전송합니다. 맞습니까?")

    check = input("y/n로 입력")

    if check == "y":
        start = time.time()  # 시간 측정 시작

        # SQL문 실행

        fail_row = []
        fail_company_id = []

        cur = conn.cursor()

        for idx in range(len(df_numpy)):

            try:
                #컬럼의 개수에 따라 {0},{1}....부분 개수 조정
                #컬럼의 개수에 따라 format함수 내의 df_numpy[idx][?] 개수 조정

                sqlString = "INSERT INTO " + table_name + " VALUES ({0}, {1}, '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}', '{11}', '{12}', {13}, '{14}')".format(
                    df_numpy[idx][0], df_numpy[idx][1], df_numpy[idx][2], df_numpy[idx][3], df_numpy[idx][4],
                    df_numpy[idx][5], df_numpy[idx][6], df_numpy[idx][7], df_numpy[idx][8], df_numpy[idx][9],
                    df_numpy[idx][10], df_numpy[idx][11], df_numpy[idx][12], df_numpy[idx][13], df_numpy[idx][14])
                cur.execute(sqlString)



            except:

                fail_row.append(idx)
                fail_company_id.append(df_numpy[idx][0])
                #전송 실패 시 , rollback !
                conn.rollback()
                break

        #실패한 행의 개수가 0이면 commit !
        if len(fail_row) == 0:
            conn.commit()
            print("------------------------------")

            print("Commit is done.")

            print("Total Duration of time: ", time.time() - start, "Seconds")  # 시간 측정 종료

            conn.close() #connection 종료

        #실패한 행의 개수가 0이 아니면 commit하지 않고 connection 종료
        else:
            print("------------------------------")

            print("[오류발견]commit을 진행하지 않습니다!")
            print("오류 행(0부터 시작): ", fail_row)
            print("오류 회사 코드: ", fail_company_id)


        conn.close() #connection 종료
        print("postgreSQL과의 연결이 종료되었습니다.")  # 시간 측정 종료



    #전송 전 'n' 입력 시, connection 종료

    elif check == "n":
        conn.close() #connection 종료
        print("------------------------------")
        print("파일을 전송하지 않습니다")
        print("postgreSQL과의 연결이 종료되었습니다.")


    # 'y', 'n' 이외의 문자 입력 시 프로그램 종료
    else:
        conn.close() #connection 종료
        print("y, n 이외의 문자가 입력되었습니다.")
        print("postgreSQL과의 연결이 종료되었습니다.")

    ########################################################################################################################
    ########################################################################################################################

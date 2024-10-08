import pandas as pd
import logging
import time
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from tqdm import tqdm  # 프로그레스바 라이브러리
from datetime import datetime, timedelta

# 로깅 설정
logging.basicConfig(filename='data_activity.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# PostgreSQL 및 MSSQL 연결 설정
pg_connection_string = "postgresql://postgres:1234@localhost:5432/tempdb"
mssql_connection_string = "mssql+pyodbc://sa:ghltktjqj7%29@221.139.49.70:2433/DJNCH?driver=SQL+Server"

# SQLAlchemy 세션 생성
pg_engine = create_engine(pg_connection_string)
mssql_engine = create_engine(mssql_connection_string, fast_executemany=True)

def get_repeat_interval():
    """MSSQL에서 TIME 값을 읽어와 초 단위로 반환"""
    try:
        with mssql_engine.connect() as mssql_conn:
            time_value = mssql_conn.execute(text("""
            SELECT TIME FROM ICT_ITMER WHERE ID = 20
            """)).scalar()

        if time_value is None:
            return 10  # 기본값 10초
        return int(time_value)

    except SQLAlchemyError as e:
        logging.error(f"TIME 값을 가져오는 중 오류 발생: {str(e)}")
        return 10  # 오류 발생 시 기본값 10초를 사용

def clear_console():
    """콘솔 화면 지우기 (Windows와 Linux/macOS 대응)"""
    import os
    os.system('cls' if os.name == 'nt' else 'clear')

while True:
    try:
        clear_console()  # 콘솔 초기화
        start_time = datetime.now()  # 수집 시작 시간 기록

        # 콘솔 및 로그에 시작 메시지 출력
        data_start_msg = "***********************************************"
        print(data_start_msg)
        logging.info(data_start_msg)

        start_msg = f"-> 데이타 수집 시작 시간 : {start_time.strftime('%Y.%m.%d %H:%M:%S')}"
        print(start_msg)
        logging.info(start_msg)

        # PostgreSQL 데이터 조회 시작 시간
        start_pg_time = time.time()

        # MSSQL에서 최대 MILKING_ID 값 조회
        with mssql_engine.connect() as mssql_conn:
            max_activity_id = mssql_conn.execute(text("""
            SELECT ISNULL(MAX(cowactivity_id), 0) FROM ICT_ACTIVITY_LOG
            """)).scalar()

        # PostgreSQL에서 해당 ID보다 큰 데이터 가져오기
        with pg_engine.connect() as pg_conn:
            query = text("""
            SELECT a.cowactivity_id
                 , a.cow_id
                 , b.cow_name
                 , a.counts
                 , a.counts_perhr
                 , a.cow_activity
                 , to_char(a.tstamp, 'YYYYMMDD') AS ymd
                 , to_char(a.tstamp, 'HH24MMSS') AS hms
              FROM tblcowactivities a
             INNER JOIN tblcows b
                    ON a.cow_id = b.cow_id
            where a.cowactivity_id > :max_activity_id
            order by a.cow_id, cowactivity_id;
            """)
            result = pg_conn.execute(query, {"max_activity_id": max_activity_id})
            data = result.fetchall()

        # PostgreSQL 데이터 조회 끝 시간
        end_pg_time = time.time()
        pg_duration = end_pg_time - start_pg_time
        pg_row_count = len(data)

        # 콘솔 및 로그에 PostgreSQL 조회 결과 출력
        pg_msg1 = f"-> postgresSQL 데이타 건수 : {pg_row_count}건"
        pg_msg2 = f"-> postgresSQL 조회에 걸린 시간 : {pg_duration:.2f}초"

        print(pg_msg1)
        logging.info(pg_msg1)

        print(pg_msg2)
        logging.info(pg_msg2)

        # 데이터가 없으면 바로 다음 반복으로 넘어가기
        if pg_row_count == 0:
            print("-> 조회된 데이터가 없습니다. MSSQL에 전송하지 않고 다음 작업을 기다립니다.")
            logging.info("조회된 데이터가 없습니다. MSSQL에 전송하지 않고 다음 작업을 기다립니다.")
            repeat_interval = get_repeat_interval()  # 반복 주기 가져오기
            next_time = datetime.now() + timedelta(seconds=repeat_interval)
            next_msg = f"-> 다음 수집 예상 시간({repeat_interval}초 후) : {next_time.strftime('%Y.%m.%d %H:%M:%S')}"
            print(next_msg)
            logging.info(next_msg)
            time.sleep(repeat_interval)
            continue  # 다음 반복으로 넘어감

        # 데이터프레임으로 변환
        df = pd.DataFrame(data, columns=["cowactivity_id", "cow_id", "cow_name", "counts", "counts_perhr", "cow_activity", "ymd", "hms"])

        # 데이터타입 변경 (필요한 경우)
        df = df.astype({
            "cowactivity_id": int,
            "cow_id": int,
            "cow_name": str,
            "counts": int,
            "counts_perhr": int,
            "cow_activity": int,
            "ymd": str,
            "hms": str,
        })

        # MSSQL 데이터 삽입 시작 시간
        start_mssql_time = time.time()

        # 데이터를 목록으로 변환
        records = df.to_dict(orient='records')

        # tqdm을 이용해 프로그레스바 생성
        with tqdm(total=len(records), desc="MSSQL 데이터 삽입 중", unit="row") as pbar:
            with mssql_engine.connect() as conn:
                try:
                    # 데이터를 삽입하고 프로그레스바 업데이트
                    for i in range(0, len(records), 100):  # 100개씩 처리
                        batch = records[i:i+100]
                        conn.execute(
                            text("""
                                INSERT INTO ICT_ACTIVITY_LOG (cowactivity_id, cow_id, cow_name, counts, counts_perhr, cow_activity, ymd, hms
                                ) VALUES (:cowactivity_id, :cow_id, :cow_name, :counts, :counts_perhr, :cow_activity, :ymd, :hms
                                )
                            """), batch)
                        pbar.update(len(batch))  # 프로그레스바 업데이트
                    conn.commit()
                except Exception as e:
                    logging.error(f"데이터 전송 실패: {str(e)}")
                    raise

        # MSSQL 데이터 삽입 끝 시간
        end_mssql_time = time.time()
        mssql_duration = end_mssql_time - start_mssql_time

        # 콘솔 및 로그에 MSSQL 결과 출력
        mssql_msg1 = f"-> MSSQL에 전송된 건수 : {len(df)}건"
        mssql_msg2 = f"-> 데이타 수집 종료 시간 : {datetime.now().strftime('%Y.%m.%d %H:%M:%S')}"

        print(mssql_msg1)
        logging.info(mssql_msg1)

        print(mssql_msg2)
        logging.info(mssql_msg2)

        # 다음 수집 시간 계산
        repeat_interval = get_repeat_interval()  # 반복 주기 가져오기
        next_time = datetime.now() + timedelta(seconds=repeat_interval)
        next_msg = f"-> 다음 수집 예상 시간({repeat_interval}초 후) : {next_time.strftime('%Y.%m.%d %H:%M:%S')}"
        print(next_msg)
        logging.info(next_msg)

    except SQLAlchemyError as e:
        logging.error(f"데이터베이스 연결 중 오류가 발생했습니다: {str(e)}")
    except Exception as e:
        logging.error(f"예기치 못한 오류가 발생했습니다: {str(e)}")

    # 대기 시간 후 다시 시작
    time.sleep(repeat_interval)

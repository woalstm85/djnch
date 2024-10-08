import pandas as pd
import logging
import time
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from tqdm import tqdm  # 프로그레스바 라이브러리
from datetime import datetime, timedelta

# 로깅 설정
logging.basicConfig(filename='data_transfer.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# PostgreSQL 및 MSSQL 연결 설정
pg_connection_string = "postgresql://postgres:1234@localhost:5432/tempdb"
#pg_connection_string = "postgresql://postgres:@localhost:5432/pvnc"
mssql_connection_string = "mssql+pyodbc://sa:ghltktjqj7%29@221.139.49.70:2433/DJNCH?driver=SQL+Server"

# SQLAlchemy 세션 생성
pg_engine = create_engine(pg_connection_string)
mssql_engine = create_engine(mssql_connection_string, fast_executemany=True)

def get_repeat_interval():
    """MSSQL에서 TIME 값을 읽어와 초 단위로 반환"""
    try:
        with mssql_engine.connect() as mssql_conn:
            time_value = mssql_conn.execute(text("""
            SELECT TIME FROM ICT_ITMER WHERE ID = 10
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
            max_milking_id = mssql_conn.execute(text("""
            SELECT ISNULL(MAX(milking_id), 0) FROM ICT_MILKING_LOG
            """)).scalar()

        # PostgreSQL에서 해당 ID보다 큰 데이터 가져오기
        with pg_engine.connect() as pg_conn:
            query = text("""
            SELECT 
                a.milking_id,
                to_char(identified_tstamp, 'YYYYMMDD') AS YMD,
                CASE 
                    WHEN to_char(identified_tstamp, 'HH24MMSS') < '120000' THEN '1' 
                    ELSE '2' 
                END AS AM_PM,
                to_char(identified_tstamp, 'HH24MMSS') AS HMS,
                a.cow_id,
                b.cow_name,
                a.milkingshift_id,
                detacher_address,
                id_tag_number_assigned,
                round(CAST(float8 (milk_weight * 0.45359) as numeric), 1) AS milk_weight,
                round(CAST(float8 (dumped_milk * 0.45359) as numeric), 1) AS dumped_milk,
                milk_conductivity,
                cow_activity,
                convertunits(c.flow_0_15_sec) AS flow_0_15_sec,
                convertunits(c.flow_15_30_sec) AS flow_15_30_sec,
                convertunits(c.flow_30_60_sec) AS flow_30_60_sec,
                convertunits(c.flow_60_120_sec) AS flow_60_120_sec,
                c.time_in_low_flow,
                c.reattach_counter,
                c.percent_expected_milk
            FROM tblmilkings AS a
            INNER JOIN public.vewcows AS b 
                ON a.cow_id = b.cow_id
            INNER JOIN public.tblstallperformances AS c 
                ON a.milking_id = c.milking_id
            WHERE id_tag_number_assigned <> ''
              AND a.milking_id > :max_milking_id
            ORDER BY a.milkingshift_id, a.identified_tstamp
            """)
            result = pg_conn.execute(query, {"max_milking_id": max_milking_id})
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
        df = pd.DataFrame(data, columns=[
            "milking_id", "YMD", "AM_PM", "HMS", "cow_id", "cow_name", "milkingshift_id",
            "detacher_address", "id_tag_number_assigned", "milk_weight", "dumped_milk",
            "milk_conductivity", "cow_activity", "flow_0_15_sec", "flow_15_30_sec",
            "flow_30_60_sec", "flow_60_120_sec", "time_in_low_flow", "reattach_counter",
            "percent_expected_milk"
        ])

        # 데이터타입 변경 (필요한 경우)
        df = df.astype({
            "milking_id": int,
            "YMD": str,
            "AM_PM": str,
            "HMS": str,
            "cow_id": int,
            "cow_name": str,
            "milkingshift_id": int,
            "detacher_address": float,
            "id_tag_number_assigned": str,
            "milk_weight": float,
            "dumped_milk": float,
            "milk_conductivity": float,
            "cow_activity": int,
            "flow_0_15_sec": float,
            "flow_15_30_sec": float,
            "flow_30_60_sec": float,
            "flow_60_120_sec": float,
            "time_in_low_flow": int,
            "reattach_counter": int,
            "percent_expected_milk": float
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
                                INSERT INTO ICT_MILKING_LOG (
                                    milking_id, YMD, AM_PM, HMS, cow_id, cow_name, milkingshift_id,
                                    detacher_address, id_tag_number_assigned, milk_weight, dumped_milk,
                                    milk_conductivity, cow_activity, flow_0_15_sec, flow_15_30_sec,
                                    flow_30_60_sec, flow_60_120_sec, time_in_low_flow, reattach_counter,
                                    percent_expected_milk
                                ) VALUES (
                                    :milking_id, :YMD, :AM_PM, :HMS, :cow_id, :cow_name, :milkingshift_id,
                                    :detacher_address, :id_tag_number_assigned, :milk_weight, :dumped_milk,
                                    :milk_conductivity, :cow_activity, :flow_0_15_sec, :flow_15_30_sec,
                                    :flow_30_60_sec, :flow_60_120_sec, :time_in_low_flow, :reattach_counter,
                                    :percent_expected_milk
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

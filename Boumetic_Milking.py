import sys
import os
import pandas as pd
import logging
from logging.handlers import TimedRotatingFileHandler
import time
from PyQt5.QtWidgets import QApplication, QMainWindow, QTextEdit, QVBoxLayout, QHBoxLayout, QWidget, QPushButton, \
    QLabel, QLineEdit, QSystemTrayIcon, QMenu, QAction
from PyQt5.QtCore import QThread, pyqtSignal, QMutex, QMutexLocker, Qt
from PyQt5.QtGui import QIcon
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, OperationalError, TimeoutError
from datetime import datetime
from PyQt5.QtCore import QTime


# 리소스 파일에 접근할 수 있도록 경로 설정 함수
def resource_path(relative_path):
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

# 로그 파일 이름 고정
log_file_name = "data_milking.log"
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler = TimedRotatingFileHandler(log_file_name, when='midnight', interval=1, backupCount=30)
log_handler.setFormatter(log_formatter)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)

# PostgreSQL 및 MSSQL 연결 설정
#pg_connection_string = "postgresql://postgres:1234@localhost:5432/tempdb"
pg_connection_string = "postgresql://postgres:@localhost:5432/pvnc"
mssql_connection_string = "mssql+pyodbc://sa:ghltktjqj7%29@221.139.49.70:2433/DJNCH?driver=SQL+Server"
pg_engine = create_engine(pg_connection_string)
mssql_engine = create_engine(mssql_connection_string, fast_executemany=True)

class DataWorker(QThread):
    update_text = pyqtSignal(str)
    finished = pyqtSignal()

    def __init__(self, interval):
        super().__init__()
        self.interval = interval
        self.stop_requested = False
        self.mutex = QMutex()

    def run(self):
        max_retries = 3  # 최대 재시도 횟수
        retry_delay = 5  # 재시도 대기 시간 (초)

        while not self.stop_requested:
            start_time = time.time()
            retries = 0  # 현재 재시도 횟수 초기화

            while retries <= max_retries and not self.stop_requested:
                try:
                    start_time_display = datetime.now().strftime('%Y.%m.%d %H:%M:%S')
                    self.update_text.emit(f"***********************************************\n")
                    self.update_text.emit(f"-> 데이터 수집 시작 시간 : {start_time_display}\n")
                    logger.info(f"데이터 수집 시작 시간 : {start_time_display}")

                    # MSSQL에서 최대 milking_id 값 조회
                    with mssql_engine.connect() as mssql_conn:
                        max_milking_id = mssql_conn.execute(
                            text("SELECT ISNULL(MAX(milking_id), 0) FROM ICT_MILKING_ORG_LOG WITH(NOLOCK)")
                        ).scalar()

                    # PostgreSQL에서 데이터 가져오기
                    with pg_engine.connect() as pg_conn:
                        query = text("""
                                    SELECT 
                                        a.milking_id,
                                        to_char(a.tstamp, 'YYYYMMDD') AS YMD,
                                        CASE 
                                            WHEN to_char(a.tstamp, 'HH24MISS') < '120000' THEN '1' 
                                            ELSE '2' 
                                        END AS AM_PM,
                                        to_char(a.tstamp, 'HH24MISS') AS HMS,
                                        a.cow_id,
                                        b.cow_number,
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
                                    ORDER BY a.milkingshift_id, a.tstamp
                        """)
                        result = pg_conn.execute(query, {"max_milking_id": max_milking_id})
                        data = result.fetchall()

                    pg_row_count = len(data)
                    pg_duration = time.time() - start_time
                    self.update_text.emit(f"-> PostgreSQL 데이터 건수: {pg_row_count}건 / {pg_duration:.2f}초\n")

                    if pg_row_count == 0:
                        self.update_text.emit("-> 조회된 데이터가 없습니다. MSSQL에 전송하지 않고 다음 작업을 기다립니다.\n")
                    else:
                        # 데이터프레임으로 변환
                        df = pd.DataFrame(data, columns=[
                            "milking_id", "YMD", "AM_PM", "HMS", "cow_id", "cow_number", "cow_name", "milkingshift_id",
                            "detacher_address", "id_tag_number_assigned", "milk_weight", "dumped_milk",
                            "milk_conductivity", "cow_activity", "flow_0_15_sec", "flow_15_30_sec",
                            "flow_30_60_sec", "flow_60_120_sec", "time_in_low_flow", "reattach_counter",
                            "percent_expected_milk"
                        ])

                        records = df.to_dict(orient='records')
                        with mssql_engine.connect() as conn:
                            postgresql_complete_time = time.time() # PostgreSQL에서 데이터를 가져온 후, 완료 시점 시간 기록
                            for i in range(0, len(records), 500):  # 500개씩 배치 처리
                                batch = records[i:i + 500]
                                for record in batch:  # 각 record를 저장 프로시저에 전달
                                    conn.execute(text("""
                                        EXEC P_ICT_MILKING_ORG_LOG_M 
                                            @milking_id=:milking_id, @ymd=:YMD, @am_pm=:AM_PM, @hms=:HMS, @cow_id=:cow_id, 
                                            @cow_number=:cow_number, @cow_name=:cow_name, @milkingshift_id=:milkingshift_id, 
                                            @detacher_address=:detacher_address, @id_tag_number_assigned=:id_tag_number_assigned, 
                                            @milk_weight=:milk_weight, @dumped_milk=:dumped_milk, @milk_conductivity=:milk_conductivity, 
                                            @cow_activity=:cow_activity, @flow_0_15_sec=:flow_0_15_sec, @flow_15_30_sec=:flow_15_30_sec, 
                                            @flow_30_60_sec=:flow_30_60_sec, @flow_60_120_sec=:flow_60_120_sec, 
                                            @time_in_low_flow=:time_in_low_flow, @reattach_counter=:reattach_counter, 
                                            @percent_expected_milk=:percent_expected_milk
                                    """), record)
                            conn.commit()  # 배치 처리 후 커밋

                        mssql_duration = time.time() - postgresql_complete_time
                        self.update_text.emit(f"-> MSSQL에 전송된 건수: {len(df)}건 / {mssql_duration:.2f}초\n")
                        self.update_text.emit(f"-> 데이터 수집 종료 시간: {datetime.now().strftime('%Y.%m.%d %H:%M:%S')}\n")
                        logger.info(f"데이터 수집 종료 시간: {datetime.now().strftime('%Y.%m.%d %H:%M:%S')}\n")
                    break  # 재시도 성공 시 루프 탈출

                # 구체적인 예외 처리 추가
                except OperationalError as e:
                    retries += 1
                    self.update_text.emit(f"-> OperationalError 발생: {str(e)}, {retries}/{max_retries} 재시도 중...\n")
                    logger.warning(f"OperationalError 발생: {str(e)}, {retries}/{max_retries} 재시도 중...")
                    time.sleep(retry_delay)

                except TimeoutError as e:
                    retries += 1
                    self.update_text.emit(f"-> TimeoutError 발생: {str(e)}, {retries}/{max_retries} 재시도 중...\n")
                    logger.warning(f"TimeoutError 발생: {str(e)}, {retries}/{max_retries} 재시도 중...")
                    time.sleep(retry_delay)

                except SQLAlchemyError as e:
                    retries += 1
                    self.update_text.emit(f"-> SQLAlchemyError 발생: {str(e)}, {retries}/{max_retries} 재시도 중...\n")
                    logger.warning(f"SQLAlchemyError 발생: {str(e)}, {retries}/{max_retries} 재시도 중...")
                    time.sleep(retry_delay)

                finally:
                    end_time = time.time()
                    elapsed_time = end_time - start_time
                    remaining_time = self.interval - elapsed_time
                    if remaining_time > 0:
                        for _ in range(int(remaining_time / 0.1)):
                            if self.stop_requested:
                                break
                            time.sleep(0.1)

        self.finished.emit()

    def stop(self):
        with QMutexLocker(self.mutex):
            self.stop_requested = True
        self.update_text.emit("-> 중지 요청이 접수되었습니다... 중지 중입니다...\n")
        logger.info("중지 요청이 접수되었습니다.")

from PyQt5.QtCore import QTimer

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("보우메틱 착유량")
        self.setGeometry(300, 300, 500, 500)

        self.setWindowFlags(Qt.Window | Qt.WindowMinimizeButtonHint | Qt.WindowCloseButtonHint)

        icon_path = resource_path("milking.png")
        self.tray_icon = QSystemTrayIcon(self)
        self.tray_icon.setIcon(QIcon(icon_path))

        tray_menu = QMenu()
        restore_action = QAction("복원", self)
        restore_action.triggered.connect(self.show_window)
        tray_menu.addAction(restore_action)

        quit_action = QAction("종료", self)
        quit_action.triggered.connect(self.quit_app)
        tray_menu.addAction(quit_action)

        self.tray_icon.setContextMenu(tray_menu)
        self.tray_icon.activated.connect(self.on_tray_icon_activated)

        self.text_edit = QTextEdit(self)
        self.text_edit.setReadOnly(True)

        # 오전 및 오후 시작/종료 시간 입력 필드 추가
        self.am_start_label = QLabel("오전 시작 시간:")
        self.am_start_hour_input = QLineEdit(self)
        self.am_start_hour_input.setFixedWidth(30)
        self.am_start_hour_input.setPlaceholderText("시")

        self.am_start_minute_input = QLineEdit(self)
        self.am_start_minute_input.setFixedWidth(30)
        self.am_start_minute_input.setPlaceholderText("분")

        # 오전 시작 시간 기본값 6:00
        self.am_start_hour_input.setText("06")
        self.am_start_minute_input.setText("00")

        self.am_end_label = QLabel("오전 종료 시간:")
        self.am_end_hour_input = QLineEdit(self)
        self.am_end_hour_input.setFixedWidth(30)
        self.am_end_hour_input.setPlaceholderText("시")

        self.am_end_minute_input = QLineEdit(self)
        self.am_end_minute_input.setFixedWidth(30)
        self.am_end_minute_input.setPlaceholderText("분")

        # 오전 종료 시간 기본값 10:00
        self.am_end_hour_input.setText("10")
        self.am_end_minute_input.setText("00")

        self.pm_start_label = QLabel("오후 시작 시간:")
        self.pm_start_hour_input = QLineEdit(self)
        self.pm_start_hour_input.setFixedWidth(30)
        self.pm_start_hour_input.setPlaceholderText("시")

        self.pm_start_minute_input = QLineEdit(self)
        self.pm_start_minute_input.setFixedWidth(30)
        self.pm_start_minute_input.setPlaceholderText("분")

        # 오후 시작 시간 기본값 15:00
        self.pm_start_hour_input.setText("15")
        self.pm_start_minute_input.setText("00")

        self.pm_end_label = QLabel("오후 종료 시간:")
        self.pm_end_hour_input = QLineEdit(self)
        self.pm_end_hour_input.setFixedWidth(30)
        self.pm_end_hour_input.setPlaceholderText("시")

        self.pm_end_minute_input = QLineEdit(self)
        self.pm_end_minute_input.setFixedWidth(30)
        self.pm_end_minute_input.setPlaceholderText("분")

        # 오후 종료 시간 기본값 19:00
        self.pm_end_hour_input.setText("19")
        self.pm_end_minute_input.setText("00")

        # 한 줄에 배치하기 위해 QHBoxLayout 사용
        time_layout = QHBoxLayout()
        time_layout.addWidget(self.am_start_label)
        time_layout.addWidget(self.am_start_hour_input)
        time_layout.addWidget(self.am_start_minute_input)
        time_layout.addSpacing(20)
        time_layout.addWidget(self.am_end_label)
        time_layout.addWidget(self.am_end_hour_input)
        time_layout.addWidget(self.am_end_minute_input)
        time_layout.addSpacing(20)
        time_layout.addWidget(self.pm_start_label)
        time_layout.addWidget(self.pm_start_hour_input)
        time_layout.addWidget(self.pm_start_minute_input)
        time_layout.addSpacing(20)
        time_layout.addWidget(self.pm_end_label)
        time_layout.addWidget(self.pm_end_hour_input)
        time_layout.addWidget(self.pm_end_minute_input)

        self.interval_label = QLabel("수집주기 (초):", self)
        self.interval_input = QLineEdit(self)
        self.interval_input.setFixedWidth(50)
        self.interval_input.setText("120")

        self.start_button = QPushButton("데이터 수집 시작", self)
        self.start_button.setFixedWidth(300)
        self.start_button.setStyleSheet("color: green;")
        self.start_button.clicked.connect(self.start_data_collection)

        self.stop_button = QPushButton("데이터 수집 정지", self)
        self.stop_button.setFixedWidth(300)
        self.stop_button.setStyleSheet("color: red;")
        self.stop_button.setEnabled(False)
        self.stop_button.clicked.connect(self.stop_data_collection)

        controls_layout = QHBoxLayout()
        controls_layout.addWidget(self.interval_label)
        controls_layout.addWidget(self.interval_input)
        controls_layout.addWidget(self.start_button)
        controls_layout.addWidget(self.stop_button)

        # 문구를 위한 QLabel 추가
        self.info_label = QLabel("* 시작/종료 시간 및 수집주기 변경 시 '데이터 수집 정지' 후 변경하세요.", self)
        self.info_label.setStyleSheet("color: red;")  # 스타일을 추가해 강조
        self.info_label.setFixedHeight(30)  # 라벨 높이 조절 (30픽셀로 고정)
        self.info_label.setContentsMargins(0, 0, 10, 10)  # 여백 설정 (좌, 상, 우, 하)

        # 기존 레이아웃에 추가
        layout = QVBoxLayout()
        layout.addWidget(self.info_label)  # 문구를 상단에 추가
        layout.addLayout(time_layout)  # 시간을 입력하는 레이아웃을 그 다음에 추가
        layout.addWidget(self.text_edit)
        layout.addLayout(controls_layout)

        container = QWidget()
        container.setLayout(layout)
        self.setCentralWidget(container)

        self.worker = None

        # 타이머 설정
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.check_time_for_auto_start_stop)
        self.timer.start(60000)

        # 프로그램 실행 후 바로 시간 체크를 한 번 호출
        self.check_time_for_auto_start_stop()  # <-- 이 부분 추가

    def check_time_for_auto_start_stop(self):
        current_time = QTime.currentTime()

        # 입력된 오전 시작 시간
        am_start_hour = int(self.am_start_hour_input.text())
        am_start_minute = int(self.am_start_minute_input.text())

        # 입력된 오전 종료 시간
        am_end_hour = int(self.am_end_hour_input.text())
        am_end_minute = int(self.am_end_minute_input.text())

        # 입력된 오후 시작 시간
        pm_start_hour = int(self.pm_start_hour_input.text())
        pm_start_minute = int(self.pm_start_minute_input.text())

        # 입력된 오후 종료 시간
        pm_end_hour = int(self.pm_end_hour_input.text())
        pm_end_minute = int(self.pm_end_minute_input.text())

        # 현재 시간이 오전 시작 시간과 종료 시간 사이일 때만 작업 시작
        if (current_time.hour() > am_start_hour or (
                current_time.hour() == am_start_hour and current_time.minute() >= am_start_minute)) and \
                (current_time.hour() < am_end_hour or (
                        current_time.hour() == am_end_hour and current_time.minute() < am_end_minute)):
            # 오전 시작 시간이 이미 지났고, 종료 시간이 지나지 않았다면 시작
            if not self.worker or not self.worker.isRunning():
                self.start_data_collection()

        # 현재 시간이 오후 시작 시간과 종료 시간 사이일 때만 작업 시작
        elif (current_time.hour() > pm_start_hour or (
                current_time.hour() == pm_start_hour and current_time.minute() >= pm_start_minute)) and \
                (current_time.hour() < pm_end_hour or (
                        current_time.hour() == pm_end_hour and current_time.minute() < pm_end_minute)):
            # 오후 시작 시간이 이미 지났고, 종료 시간이 지나지 않았다면 시작
            if not self.worker or not self.worker.isRunning():
                self.start_data_collection()

        # 현재 시간이 오전 종료 시간을 지났을 경우 멈춤
        elif current_time.hour() > am_end_hour or (
                current_time.hour() == am_end_hour and current_time.minute() >= am_end_minute):
            if self.worker and self.worker.isRunning():
                self.stop_data_collection()

        # 현재 시간이 오후 종료 시간을 지났을 경우 멈춤
        elif current_time.hour() > pm_end_hour or (
                current_time.hour() == pm_end_hour and current_time.minute() >= pm_end_minute):
            if self.worker and self.worker.isRunning():
                self.stop_data_collection()

    def closeEvent(self, event):
        event.ignore()
        self.hide()
        self.tray_icon.show()

    def on_tray_icon_activated(self, reason):
        if reason == QSystemTrayIcon.DoubleClick:
            self.show_window()

    def show_window(self):
        self.show()
        self.tray_icon.hide()

    def quit_app(self):
        QApplication.quit()

    def start_data_collection(self):
        try:
            interval = int(self.interval_input.text())
            if interval < 10:
                raise ValueError
        except ValueError:
            self.append_text("수집 주기는 10초 이상의 양수로 입력해주세요.\n")
            return

        self.start_button.setEnabled(False)
        self.stop_button.setEnabled(True)

        self.worker = DataWorker(interval)
        self.worker.update_text.connect(self.append_text)
        self.worker.finished.connect(self.on_data_collection_finished)
        self.worker.start()

    def stop_data_collection(self):
        if self.worker:
            self.worker.stop()

    def on_data_collection_finished(self):
        self.start_button.setEnabled(True)
        self.stop_button.setEnabled(False)
        self.append_text("-> 데이터 수집이 정지되었습니다.\n")

    def append_text(self, text):
        self.text_edit.append(text)
        self.text_edit.ensureCursorVisible()

        max_lines = 500
        if self.text_edit.document().blockCount() > max_lines:
            cursor = self.text_edit.textCursor()
            cursor.movePosition(cursor.Start)
            cursor.select(cursor.BlockUnderCursor)
            cursor.removeSelectedText()
            cursor.deleteChar()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    icon_path = resource_path("milking.png")
    app.setWindowIcon(QIcon(icon_path))
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())

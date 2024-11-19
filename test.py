import sys
import os
import pandas as pd
import logging
import json
from logging.handlers import TimedRotatingFileHandler
import time
from PyQt5.QtWidgets import QApplication, QMainWindow, QTextEdit, QVBoxLayout, QHBoxLayout, QWidget, QPushButton, \
    QLabel, QLineEdit, QSystemTrayIcon, QMenu, QAction, QProgressBar
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

# 로그 파일 설정
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
    update_progress = pyqtSignal(int)  # 프로그레스바 업데이트 신호 추가
    finished = pyqtSignal()

    def __init__(self, interval, am_end_time, pm_end_time):
        super().__init__()
        self.interval = interval
        self.am_end_time = am_end_time
        self.pm_end_time = pm_end_time
        self.stop_requested = False
        self.mutex = QMutex()

    def run(self):
        max_retries = 3
        retry_delay = 10

        while not self.stop_requested:
            # 현재 시간이 설정된 종료 시간을 지났다면 자동으로 멈춤
            current_time = QTime.currentTime()
            if current_time >= self.am_end_time and current_time < QTime(12, 0):
                self.update_text.emit("-> 오전 종료 시간에 도달하여 데이터 수집을 중지합니다.\n")
                break
            elif current_time >= self.pm_end_time:
                self.update_text.emit("-> 오후 종료 시간에 도달하여 데이터 수집을 중지합니다.\n")
                break

            start_time = time.time()
            retries = 0  # 현재 재시도 횟수 초기화

            while retries < max_retries and not self.stop_requested:
                try:
                    start_time_display = datetime.now().strftime('%Y.%m.%d %H:%M:%S')
                    self.update_text.emit(f"***********************************************\n")
                    self.update_text.emit(f"-> 데이터 수집 시작 시간 : {start_time_display}\n")
                    logger.info(f"데이터 수집 시작 시간 : {start_time_display}")

                    with mssql_engine.connect() as mssql_conn:
                        max_tstamp = mssql_conn.execute(
                            text("SELECT ISNULL(MAX(tstamp), CAST('1900-01-01 00:00:00.000000' AS DATETIME2(6)))  FROM ICT_MILKING_ORG_LOG WITH(NOLOCK)")
                        ).scalar()

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
                                        c.percent_expected_milk,
                                        to_char(a.tstamp, 'YYYY-MM-DD HH24:MI:SS.US') AS tstamp_string
                                    FROM tblmilkings AS a
                                    LEFT OUTER JOIN public.vewcows AS b 
                                        ON a.cow_id = b.cow_id
                                    LEFT OUTER JOIN public.tblstallperformances AS c 
                                        ON a.milking_id = c.milking_id
                                    WHERE id_tag_number_assigned <> ''
                                      AND a.tstamp > :max_tstamp
                                    ORDER BY a.milkingshift_id, a.tstamp
                        """)
                        result = pg_conn.execute(query, {"max_tstamp": max_tstamp})
                        data = result.fetchall()

                    pg_row_count = len(data)
                    pg_duration = time.time() - start_time
                    self.update_text.emit(f"-> PostgreSQL 데이터 건수: {pg_row_count}건 / {pg_duration:.2f}초\n")

                    if pg_row_count == 0:
                        self.update_text.emit("-> 조회된 데이터가 없습니다. MSSQL에 전송하지 않고 다음 작업을 기다립니다.\n")
                    else:
                        df = pd.DataFrame(data, columns=[
                            "milking_id", "YMD", "AM_PM", "HMS", "cow_id", "cow_number", "cow_name", "milkingshift_id",
                            "detacher_address", "id_tag_number_assigned", "milk_weight", "dumped_milk",
                            "milk_conductivity", "cow_activity", "flow_0_15_sec", "flow_15_30_sec",
                            "flow_30_60_sec", "flow_60_120_sec", "time_in_low_flow", "reattach_counter",
                            "percent_expected_milk", "tstamp_string"
                        ])

                        records = df.to_dict(orient='records')
                        with mssql_engine.connect() as conn:
                            postgresql_complete_time = time.time()
                            transaction_started = False

                            try:
                                for i in range(0, len(records), 500):
                                    batch = records[i:i + 500]

                                    if not transaction_started:
                                        conn.begin()
                                        transaction_started = True

                                    for record in batch:
                                        conn.execute(text("""
                                        EXEC P_ICT_MILKING_ORG_LOG_M 
                                            @milking_id=:milking_id, @ymd=:YMD, @am_pm=:AM_PM, @hms=:HMS, @cow_id=:cow_id, 
                                            @cow_number=:cow_number, @cow_name=:cow_name, @milkingshift_id=:milkingshift_id, 
                                            @detacher_address=:detacher_address, @id_tag_number_assigned=:id_tag_number_assigned, 
                                            @milk_weight=:milk_weight, @dumped_milk=:dumped_milk, @milk_conductivity=:milk_conductivity, 
                                            @cow_activity=:cow_activity, @flow_0_15_sec=:flow_0_15_sec, @flow_15_30_sec=:flow_15_30_sec, 
                                            @flow_30_60_sec=:flow_30_60_sec, @flow_60_120_sec=:flow_60_120_sec, 
                                            @time_in_low_flow=:time_in_low_flow, @reattach_counter=:reattach_counter, 
                                            @percent_expected_milk=:percent_expected_milk, @tstamp=:tstamp_string
                                        """), record)

                                    progress = int((i + len(batch)) / len(records) * 100)
                                    self.update_progress.emit(progress)

                                conn.commit()
                            except SQLAlchemyError as e:
                                if transaction_started:
                                    conn.rollback()
                                raise e

                        mssql_duration = time.time() - postgresql_complete_time
                        self.update_text.emit(f"-> MSSQL에 전송된 건수: {len(df)}건 / {mssql_duration:.2f}초\n")
                        self.update_text.emit(f"-> 데이터 수집 종료 시간: {datetime.now().strftime('%Y.%m.%d %H:%M:%S')}\n")
                        logger.info(f"데이터 수집 종료 시간: {datetime.now().strftime('%Y.%m.%d %H:%M:%S')}\n")
                    break

                except (OperationalError, TimeoutError, SQLAlchemyError) as e:
                    retries += 1
                    self.update_text.emit(f"-> {type(e).__name__} 발생: {str(e)}, {retries}/{max_retries} 재시도 중...\n")
                    logger.warning(f"{type(e).__name__} 발생: {str(e)}, {retries}/{max_retries} 재시도 중...")

                    if retries < max_retries:
                        time.sleep(retry_delay)

            elapsed_time = time.time() - start_time
            remaining_time = self.interval - elapsed_time
            if remaining_time > 0:
                sleep_duration = 0.1
                for _ in range(int(remaining_time / sleep_duration)):
                    if self.stop_requested:
                        break
                    time.sleep(sleep_duration)

        self.finished.emit()

    def stop(self):
        with QMutexLocker(self.mutex):
            self.stop_requested = True
        self.update_text.emit("-> 중지 요청이 접수되었습니다... 중지 중입니다...\n")
        logger.info("중지 요청이 접수되었습니다.")

from PyQt5.QtCore import QTimer

class MainWindow(QMainWindow):
    CONFIG_FILE = "config.json"

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

        # 설정 파일에서 기본값 불러오기
        self.load_config()

        # UI 설정
        self.setup_ui()

        self.worker = None

        # 타이머 설정
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.check_time_for_auto_start_stop)
        self.timer.start(60000)

        self.check_time_for_auto_start_stop()

    def load_config(self):
        try:
            with open("config.json", "r") as file:
                config = json.load(file)
                self.am_start_hour = config.get("am_start_hour", "06")
                self.am_start_minute = config.get("am_start_minute", "00")
                self.am_end_hour = config.get("am_end_hour", "10")
                self.am_end_minute = config.get("am_end_minute", "00")
                self.pm_start_hour = config.get("pm_start_hour", "15")
                self.pm_start_minute = config.get("pm_start_minute", "00")
                self.pm_end_hour = config.get("pm_end_hour", "19")
                self.pm_end_minute = config.get("pm_end_minute", "00")
                self.interval = config.get("interval", 120)
        except (FileNotFoundError, json.JSONDecodeError):
            # 파일이 없거나 JSON 포맷 오류 시 기본값 사용
            self.am_start_hour = "06"
            self.am_start_minute = "00"
            self.am_end_hour = "10"
            self.am_end_minute = "00"
            self.pm_start_hour = "15"
            self.pm_start_minute = "00"
            self.pm_end_hour = "19"
            self.pm_end_minute = "00"
            self.interval = 120

    def save_config(self):
        config = {
            "am_start_hour": self.am_start_hour_input.text(),
            "am_start_minute": self.am_start_minute_input.text(),
            "am_end_hour": self.am_end_hour_input.text(),
            "am_end_minute": self.am_end_minute_input.text(),
            "pm_start_hour": self.pm_start_hour_input.text(),
            "pm_start_minute": self.pm_start_minute_input.text(),
            "pm_end_hour": self.pm_end_hour_input.text(),
            "pm_end_minute": self.pm_end_minute_input.text(),
            "interval": int(self.interval_input.text())
        }
        with open("config.json", "w") as file:
            json.dump(config, file, indent=4)

    def setup_ui(self):
        # 오전 및 오후 시작/종료 시간 입력 필드
        self.am_start_hour_input = QLineEdit(self.am_start_hour)
        self.am_start_minute_input = QLineEdit(self.am_start_minute)
        self.am_end_hour_input = QLineEdit(self.am_end_hour)
        self.am_end_minute_input = QLineEdit(self.am_end_minute)
        self.pm_start_hour_input = QLineEdit(self.pm_start_hour)
        self.pm_start_minute_input = QLineEdit(self.pm_start_minute)
        self.pm_end_hour_input = QLineEdit(self.pm_end_hour)
        self.pm_end_minute_input = QLineEdit(self.pm_end_minute)

        # 프로그레스바 추가
        self.progress_bar = QProgressBar(self)
        self.progress_bar.setValue(0)  # 초기값 설정

        # 레이아웃 설정
        layout = QVBoxLayout()
        time_layout = QHBoxLayout()

        # 오전 및 오후 시작/종료 시간 필드 레이아웃 추가
        time_layout.addWidget(QLabel("오전 시작 시간:"))
        time_layout.addWidget(self.am_start_hour_input)
        time_layout.addWidget(self.am_start_minute_input)
        time_layout.addSpacing(20)
        time_layout.addWidget(QLabel("오전 종료 시간:"))
        time_layout.addWidget(self.am_end_hour_input)
        time_layout.addWidget(self.am_end_minute_input)
        time_layout.addSpacing(20)
        time_layout.addWidget(QLabel("오후 시작 시간:"))
        time_layout.addWidget(self.pm_start_hour_input)
        time_layout.addWidget(self.pm_start_minute_input)
        time_layout.addSpacing(20)
        time_layout.addWidget(QLabel("오후 종료 시간:"))
        time_layout.addWidget(self.pm_end_hour_input)
        time_layout.addWidget(self.pm_end_minute_input)

        # 수집주기 및 버튼 레이아웃
        controls_layout = QHBoxLayout()
        controls_layout.addWidget(QLabel("수집주기 (초):"))
        self.interval_input = QLineEdit(str(self.interval))  # int 값을 문자열로 변환
        self.interval_input.setFixedWidth(50)
        controls_layout.addWidget(self.interval_input)

        # 버튼 스타일 및 기능 설정
        self.start_button = QPushButton("데이터 수집 시작")
        self.start_button.setStyleSheet("color: green;")  # 텍스트 색상
        self.start_button.setFixedWidth(330)
        self.start_button.clicked.connect(self.start_data_collection)
        controls_layout.addWidget(self.start_button)

        self.stop_button = QPushButton("데이터 수집 정지")
        self.stop_button.setStyleSheet("color: red;")  # 텍스트 색상
        self.stop_button.setFixedWidth(330)
        self.stop_button.setEnabled(False)
        self.stop_button.clicked.connect(self.stop_data_collection)
        controls_layout.addWidget(self.stop_button)

        # 추가 정보 및 문구 표시
        self.info_label = QLabel("* 시작/종료 시간 및 수집주기 변경 시 '데이터 수집 정지' 후 변경하세요.")
        self.info_label.setStyleSheet("color: red;")  # 텍스트 색상
        self.info_label.setFixedHeight(30)
        self.info_label.setContentsMargins(0, 0, 10, 10)

        layout.addWidget(self.info_label)  # 정보 라벨 추가
        layout.addLayout(time_layout)
        layout.addWidget(self.text_edit)
        layout.addWidget(self.progress_bar)  # 프로그레스바 추가
        layout.addLayout(controls_layout)

        container = QWidget()
        container.setLayout(layout)
        self.setCentralWidget(container)

    # 창을 복원하는 show_window 메서드 추가
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

    def start_data_collection(self):
        try:
            interval = int(self.interval_input.text())
            if interval < 10:
                raise ValueError
        except ValueError:
            self.append_text("수집 주기는 10초 이상의 양수로 입력해주세요.\n")
            return

        self.save_config()  # 현재 설정을 JSON 파일에 저장

        # 오전 종료 시간과 오후 종료 시간을 QTime으로 변환하여 전달
        am_end_time = QTime(int(self.am_end_hour_input.text()), int(self.am_end_minute_input.text()))
        pm_end_time = QTime(int(self.pm_end_hour_input.text()), int(self.pm_end_minute_input.text()))

        self.start_button.setEnabled(False)
        self.stop_button.setEnabled(True)
        self.progress_bar.setValue(0)  # 프로그레스바 초기화

        self.worker = DataWorker(interval, am_end_time, pm_end_time)
        self.worker.update_text.connect(self.append_text)
        self.worker.update_progress.connect(self.progress_bar.setValue)  # 프로그레스바 업데이트
        self.worker.finished.connect(self.on_data_collection_finished)
        self.worker.start()

    def stop_data_collection(self):
        if self.worker:
            self.worker.stop()

    def on_data_collection_finished(self):
        self.start_button.setEnabled(True)
        self.stop_button.setEnabled(False)
        self.progress_bar.setValue(0)  # 완료 후 프로그레스바 초기화
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

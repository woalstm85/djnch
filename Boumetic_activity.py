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
log_file_name = "data_activity.log"
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
                        max_activity_id = mssql_conn.execute(
                            text("SELECT ISNULL(MAX(cowactivity_id), 0) FROM ICT_ACTIVITY_LOG")
                        ).scalar()

                    # PostgreSQL에서 데이터 가져오기
                    with pg_engine.connect() as pg_conn:
                        query = text("""
                                SELECT a.cowactivity_id
                                     , a.cow_id
                                     , b.cow_number
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

                    pg_row_count = len(data)
                    pg_duration = time.time() - start_time
                    self.update_text.emit(f"-> PostgreSQL 데이터 건수: {pg_row_count}건 / {pg_duration:.2f}초\n")

                    if pg_row_count == 0:
                        self.update_text.emit("-> 조회된 데이터가 없습니다. MSSQL에 전송하지 않고 다음 작업을 기다립니다.\n")
                    else:
                        df = pd.DataFrame(data, columns=["cowactivity_id", "cow_id", "cow_number", "cow_name", "counts", "counts_perhr", "cow_activity", "ymd", "hms"])
                        start_mssql_time = time.time()

                        records = df.to_dict(orient='records')
                        with mssql_engine.connect() as conn:
                            for i in range(0, len(records), 500):
                                batch = records[i:i + 500]
                                conn.execute(text("""
                                INSERT INTO ICT_ACTIVITY_LOG (cowactivity_id, cow_id, cow_number, cow_name, counts, counts_perhr, cow_activity, ymd, hms
                                ) VALUES (:cowactivity_id, :cow_id, :cow_number, :cow_name, :counts, :counts_perhr, :cow_activity, :ymd, :hms)
                                """), batch)
                            conn.commit()  # 커밋을 명시적으로 호출
                        mssql_duration = time.time() - start_mssql_time
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
        self.setWindowTitle("Boumetic Activity_log")
        self.setGeometry(300, 300, 500, 500)

        self.setWindowFlags(Qt.Window | Qt.WindowMinimizeButtonHint | Qt.WindowCloseButtonHint)

        icon_path = resource_path("activity.png")
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

        self.interval_label = QLabel("수집주기 (초):", self)
        self.interval_input = QLineEdit(self)
        self.interval_input.setFixedWidth(50)
        self.interval_input.setText("10")

        self.start_button = QPushButton("데이터 수집 시작", self)
        self.start_button.setFixedWidth(200)
        self.start_button.setStyleSheet("color: green;")
        self.start_button.clicked.connect(self.start_data_collection)

        self.stop_button = QPushButton("데이터 수집 정지", self)
        self.stop_button.setFixedWidth(200)
        self.stop_button.setStyleSheet("color: red;")
        self.stop_button.setEnabled(False)
        self.stop_button.clicked.connect(self.stop_data_collection)

        controls_layout = QHBoxLayout()
        controls_layout.addWidget(self.interval_label)
        controls_layout.addWidget(self.interval_input)
        controls_layout.addWidget(self.start_button)
        controls_layout.addWidget(self.stop_button)

        layout = QVBoxLayout()
        layout.addWidget(self.text_edit)
        layout.addLayout(controls_layout)

        container = QWidget()
        container.setLayout(layout)
        self.setCentralWidget(container)

        self.worker = None

        # 15:00에 자동으로 시작하고, 18:00에 자동으로 중지하기 위한 타이머
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.check_time_for_auto_start_stop)
        self.timer.start(60000)  # 60초마다 시간 체크

    def check_time_for_auto_start_stop(self):
        current_time = QTime.currentTime()

        # 13:00에 자동 시작
        if current_time.hour() == 15 and current_time.minute() == 00:
            if not self.worker or not self.worker.isRunning():
                self.start_data_collection()

        # 18:00에 자동 중지
        if current_time.hour() == 18 and current_time.minute() == 30:
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
    icon_path = resource_path("activity.png")
    app.setWindowIcon(QIcon(icon_path))
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())

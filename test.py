import sys
import json
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout,
                             QHBoxLayout, QPushButton, QLabel, QLineEdit,
                             QProgressBar, QTextEdit, QMessageBox, QFrame, QGroupBox,
                             QSystemTrayIcon, QMenu, QAction)
from PyQt5.QtCore import QTimer, QTime
from PyQt5.QtGui import QIcon
from PyQt5 import QtCore
from sqlalchemy import create_engine, text



class DataCollectorApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle('보우메틱 활동량')
        self.setGeometry(100, 100, 800, 500)  # 높이를 조금 줄임

        # 필수 변수 초기화
        self.collection_active = False
        self.auto_mode = True
        self.manual_stop = False
        self.is_quitting = False

        # 트레이 아이콘 설정
        self.tray_icon = QSystemTrayIcon(self)
        self.tray_icon.setIcon(QIcon('activity.ico'))

        # 초기 툴팁 설정
        self.update_tray_tooltip()  # 이제 collection_active가 초기화된 후에 호출

        # 트레이 메뉴 설정
        tray_menu = QMenu()

        # 창 보이기 액션
        show_action = QAction("열기", self)
        show_action.triggered.connect(self.show_window)
        tray_menu.addAction(show_action)

        # 구분선 추가
        tray_menu.addSeparator()

        # 종료 액션
        quit_action = QAction("종료", self)
        quit_action.triggered.connect(self.quit_application)
        tray_menu.addAction(quit_action)

        # 메뉴를 트레이 아이콘에 설정
        self.tray_icon.setContextMenu(tray_menu)

        # 트레이 아이콘 더블클릭 시 창 보이기
        self.tray_icon.activated.connect(self.tray_icon_activated)

        # 트레이 아이콘 표시
        self.tray_icon.show()

        # 상태 업데이트
        self.update_tray_tooltip()

        # 창 닫기 이벤트 처리를 위한 플래그


        # 설정 파일 로드를 먼저 수행
        self.config_file = 'config2.json'
        self.load_config()

        # 타이머 초기화
        self.check_timer = QTimer()
        self.check_timer.timeout.connect(self.check_time_range)
        self.check_timer.start(1000)  # 1초마다 시간 체크

        self.collection_timer = QTimer()
        self.collection_timer.timeout.connect(self.collect_data)

        # 데이터베이스 연결 설정
        self.pg_engine = create_engine("postgresql://postgres:1234@localhost:5432/tempdb")
        self.ms_engine = create_engine("mssql+pyodbc://sa:ghltktjqj7%29@221.139.49.70:2433/DJNCH?driver=SQL+Server")

        # 설정 파일 로드
        self.load_config()

        # UI 설정
        self.setup_ui()

    def setup_ui(self):
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        layout = QVBoxLayout(main_widget)
        layout.setSpacing(10)  # 위젯 간 간격 설정

        # 시간 설정 그룹박스
        time_group = QGroupBox("수집 시간 설정")
        time_group.setStyleSheet("""
            QGroupBox {
                font-weight: bold;
                border: 1px solid #3F3F3F;
                margin-top: 6px;
                padding-top: 10px;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                padding: 0 5px;
            }
        """)
        time_layout = QHBoxLayout(time_group)

        # 오전/오후 시간 입력 스타일
        time_input_style = """
            QLineEdit {
                border: 1px solid #3F3F3F;
                padding: 3px;
                background: #ffffff;
                text-align: center;  /* 텍스트 중앙 정렬 */
            }
            QLabel {
                padding: 0px 5px;
            }
        """

        # 오전 시간 프레임
        morning_frame = QFrame()
        morning_frame.setFrameStyle(QFrame.Panel | QFrame.Raised)
        morning_frame.setStyleSheet(time_input_style)
        # 오전 시간 프레임의 레이아웃 부분 수정
        morning_layout = QHBoxLayout(morning_frame)
        morning_layout.setContentsMargins(10, 5, 10, 5)
        morning_layout.setSpacing(5)  # 위젯 간 간격 설정

        # 레이아웃 정렬을 위해 QLabel 너비 고정
        morning_start_label = QLabel('오전 시작:')
        morning_start_label.setFixedWidth(70)
        morning_layout.addWidget(morning_start_label)

        self.morning_start_hour = QLineEdit(self.config['morning_start']['hour'])
        self.morning_start_hour.setFixedWidth(40)
        self.morning_start_hour.setAlignment(QtCore.Qt.AlignCenter)
        morning_layout.addWidget(self.morning_start_hour)
        morning_layout.addWidget(QLabel(':'), 1)
        self.morning_start_min = QLineEdit(self.config['morning_start']['minute'])
        self.morning_start_min.setFixedWidth(40)
        self.morning_start_min.setAlignment(QtCore.Qt.AlignCenter)
        morning_layout.addWidget(self.morning_start_min)

        morning_layout.addSpacing(20)  # 시작과 종료 시간 사이 간격

        morning_end_label = QLabel('오전 종료:')
        morning_end_label.setFixedWidth(70)
        morning_layout.addWidget(morning_end_label)

        self.morning_end_hour = QLineEdit(self.config['morning_end']['hour'])
        self.morning_end_hour.setFixedWidth(40)
        self.morning_end_hour.setAlignment(QtCore.Qt.AlignCenter)
        morning_layout.addWidget(self.morning_end_hour)
        morning_layout.addWidget(QLabel(':'), 1)
        self.morning_end_min = QLineEdit(self.config['morning_end']['minute'])
        self.morning_end_min.setFixedWidth(40)
        self.morning_end_min.setAlignment(QtCore.Qt.AlignCenter)
        morning_layout.addWidget(self.morning_end_min)

        # 오후 시간 프레임
        afternoon_frame = QFrame()
        afternoon_frame.setFrameStyle(QFrame.Panel | QFrame.Raised)
        afternoon_frame.setStyleSheet(time_input_style)
        afternoon_layout = QHBoxLayout(afternoon_frame)
        afternoon_layout.setContentsMargins(10, 5, 10, 5)

        afternoon_layout.addWidget(QLabel('오후 시작:'))
        self.afternoon_start_hour = QLineEdit(self.config['afternoon_start']['hour'])
        self.afternoon_start_hour.setFixedWidth(40)
        self.afternoon_start_hour.setAlignment(QtCore.Qt.AlignCenter)
        afternoon_layout.addWidget(self.afternoon_start_hour)
        afternoon_layout.addWidget(QLabel(':'))
        self.afternoon_start_min = QLineEdit(self.config['afternoon_start']['minute'])
        self.afternoon_start_min.setFixedWidth(40)
        self.afternoon_start_min.setAlignment(QtCore.Qt.AlignCenter)
        afternoon_layout.addWidget(self.afternoon_start_min)

        afternoon_layout.addWidget(QLabel('오후 종료:'))
        self.afternoon_end_hour = QLineEdit(self.config['afternoon_end']['hour'])
        self.afternoon_end_hour.setFixedWidth(40)
        self.afternoon_end_hour.setAlignment(QtCore.Qt.AlignCenter)
        afternoon_layout.addWidget(self.afternoon_end_hour)
        afternoon_layout.addWidget(QLabel(':'))
        self.afternoon_end_min = QLineEdit(self.config['afternoon_end']['minute'])
        self.afternoon_end_min.setFixedWidth(40)
        self.afternoon_end_min.setAlignment(QtCore.Qt.AlignCenter)
        afternoon_layout.addWidget(self.afternoon_end_min)

        time_layout.addWidget(morning_frame)
        time_layout.addWidget(afternoon_frame)
        layout.addWidget(time_group)

        # 로그 영역
        log_group = QGroupBox("수집 상태")
        log_group.setStyleSheet("""
            QGroupBox {
                font-weight: bold;
                border: 1px solid #3F3F3F;
                margin-top: 6px;
                padding-top: 10px;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                padding: 0 5px;
            }            
        """)
        log_layout = QVBoxLayout(log_group)

        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setStyleSheet("""
            QTextEdit {
                border: 1px solid #3F3F3F;
                background-color: #ffffff;
                font-family: Consolas, monospace;
            }
        """)
        log_layout.addWidget(self.log_text)

        # 진행률 바 스타일 설정
        self.progress_bar = QProgressBar()
        self.progress_bar.setStyleSheet("""
            QProgressBar {
                border: 1px solid #3F3F3F;

                text-align: center;
                height: 25px;
            }
            QProgressBar::chunk {
                background-color: #4CAF50;
            }
        """)
        log_layout.addWidget(self.progress_bar)

        layout.addWidget(log_group)

        # 컨트롤 영역
        control_frame = QFrame()
        # setup_ui 메서드의 control_frame 스타일시트 부분 수정
        control_frame.setStyleSheet("""
            QFrame {
                border: 1px solid #3F3F3F;
                background-color: #f5f5f5;
            }
            QPushButton {
                min-width: 100px;
                min-height: 30px;
                padding: 5px;
                border: none;
                color: white;
                cursor: pointer;  /* 마우스 커서 변경 */
            }
            QPushButton:enabled {
                background-color: #2196F3;
            }
            QPushButton:disabled {
                background-color: #cccccc;
                cursor: default;  /* 비활성화 시 기본 커서 */
            }
            QPushButton:hover:enabled {
                background-color: #1976D2;
                cursor: pointer;  /* 호버 시 마우스 커서 */
            }
            QLineEdit {
                border: 1px solid #999999;
                padding: 3px;
            }
        """)
        control_layout = QHBoxLayout(control_frame)
        control_layout.setContentsMargins(15, 10, 15, 10)

        # 빈 공간을 먼저 추가하여 나머지 요소들을 오른쪽으로 밀기
        control_layout.addStretch()

        # 수집주기 설정
        interval_layout = QHBoxLayout()
        interval_layout.addWidget(QLabel(' 수집주기 (초) : '))
        self.interval_input = QLineEdit(self.config['interval'])
        self.interval_input.setFixedWidth(50)
        self.interval_input.setFixedHeight(40)
        self.interval_input.setAlignment(QtCore.Qt.AlignCenter)
        interval_layout.addWidget(self.interval_input)
        interval_layout.addSpacing(20)  # 수집주기 input 뒤에 여백 추가

        control_layout.addStretch()  # 왼쪽 빈 공간
        control_layout.addLayout(interval_layout)  # 수집주기 레이아웃


        # 버튼들
        button_layout = QHBoxLayout()
        self.start_button = QPushButton('데이터 수집 시작')
        self.start_button.setStyleSheet("""
            QPushButton {
                background-color: #4CAF50;
                cursor: pointer;
            }
            QPushButton:hover:enabled {
                background-color: #45a049;
            }
            QPushButton:disabled {
                background-color: #cccccc;
                cursor: default;
            }
        """)
        button_layout.addWidget(self.start_button)

        self.stop_button = QPushButton('데이터 수집 정지')
        self.stop_button.setStyleSheet("""
            QPushButton {
                background-color: #f44336;
                cursor: pointer;
            }
            QPushButton:hover:enabled {
                background-color: #da190b;
            }
            QPushButton:disabled {
                background-color: #cccccc;
                cursor: default;
            }
        """)
        self.stop_button.setEnabled(False)
        button_layout.addWidget(self.stop_button)

        self.save_button = QPushButton('설정 저장')
        self.save_button.setEnabled(False)
        button_layout.addWidget(self.save_button)

        control_layout.addLayout(interval_layout)
        control_layout.addLayout(button_layout)

        layout.addWidget(control_frame)

        # 이벤트 연결
        self.start_button.clicked.connect(self.manual_start_collection)
        self.stop_button.clicked.connect(self.stop_collection)
        self.save_button.clicked.connect(self.save_settings)

        # setup_ui 메서드 내의 시간 입력 설정 부분에 다음 내용 추가
        self.morning_start_hour.textChanged.connect(self.on_setting_changed)
        self.morning_start_min.textChanged.connect(self.on_setting_changed)
        self.morning_end_hour.textChanged.connect(self.on_setting_changed)
        self.morning_end_min.textChanged.connect(self.on_setting_changed)
        self.afternoon_start_hour.textChanged.connect(self.on_setting_changed)
        self.afternoon_start_min.textChanged.connect(self.on_setting_changed)
        self.afternoon_end_hour.textChanged.connect(self.on_setting_changed)
        self.afternoon_end_min.textChanged.connect(self.on_setting_changed)
        self.interval_input.textChanged.connect(self.on_setting_changed)

        # setup_ui 메서드의 버튼 생성 부분에 추가
        from PyQt5.QtCore import Qt

        self.start_button.setCursor(Qt.PointingHandCursor)
        self.stop_button.setCursor(Qt.PointingHandCursor)
        self.save_button.setCursor(Qt.PointingHandCursor)

    def on_setting_changed(self):
        """설정값이 변경되면 호출되는 메서드"""
        if self.collection_active:
            QMessageBox.information(self, "설정 변경", "설정이 변경되어 수집이 중지됩니다.\n저장 후 수동으로 다시 시작해주세요.")
            self.stop_collection()

        # 설정 저장 버튼 활성화
        self.save_button.setEnabled(True)
        # 변경사항 저장 전까지 시작 버튼 비활성화
        self.start_button.setEnabled(False)

    def update_button_states(self):
        """수집 상태에 따른 버튼 활성화/비활성화 관리"""
        if self.collection_active:
            self.start_button.setEnabled(False)
            self.stop_button.setEnabled(True)
            self.save_button.setEnabled(False)  # 수집 중에는 설정 저장 불가
        else:
            self.stop_button.setEnabled(False)
            # 설정이 변경되었다면 시작 버튼 비활성화
            if self.save_button.isEnabled():
                self.start_button.setEnabled(False)
            else:
                self.start_button.setEnabled(True)

    def log_message(self, message):
        current_time = QTime.currentTime().toString("HH:mm:ss")
        self.log_text.append(f"[{current_time}] {message}")

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
        except FileNotFoundError:
            # 기본 설정
            self.config = {
                'morning_start': {'hour': '08', 'minute': '00'},
                'morning_end': {'hour': '10', 'minute': '00'},
                'afternoon_start': {'hour': '18', 'minute': '00'},
                'afternoon_end': {'hour': '21', 'minute': '00'},
                'interval': '30'
            }
            self.save_config()

    def save_config(self):
        config = {
            'morning_start': {'hour': self.morning_start_hour.text(), 'minute': self.morning_start_min.text()},
            'morning_end': {'hour': self.morning_end_hour.text(), 'minute': self.morning_end_min.text()},
            'afternoon_start': {'hour': self.afternoon_start_hour.text(), 'minute': self.afternoon_start_min.text()},
            'afternoon_end': {'hour': self.afternoon_end_hour.text(), 'minute': self.afternoon_end_min.text()},
            'interval': self.interval_input.text()
        }
        with open(self.config_file, 'w') as f:
            json.dump(config, f, indent=4)

    def save_settings(self):
        """설정 저장 버튼 클릭 시 호출되는 메서드"""
        try:
            # 시간 형식 검증
            for time_input in [self.morning_start_hour, self.morning_end_hour,
                               self.afternoon_start_hour, self.afternoon_end_hour]:
                if not (0 <= int(time_input.text()) <= 23):
                    raise ValueError("시간은 0-23 사이의 값이어야 합니다.")

            for time_input in [self.morning_start_min, self.morning_end_min,
                               self.afternoon_start_min, self.afternoon_end_min]:
                if not (0 <= int(time_input.text()) <= 59):
                    raise ValueError("분은 0-59 사이의 값이어야 합니다.")

            interval = int(self.interval_input.text())
            if not (1 <= interval <= 3600):
                raise ValueError("수집 주기는 1-3600초 사이의 값이어야 합니다.")

            self.save_config()
            self.save_button.setEnabled(False)
            self.start_button.setEnabled(True)  # 저장 완료 후 시작 버튼 활성화
            self.log_message("설정이 저장되었습니다. 수집을 다시 시작할 수 있습니다.")
        except ValueError as e:
            QMessageBox.warning(self, "설정 오류", str(e))
        except Exception as e:
            self.log_message(f"설정 저장 중 오류 발생: {str(e)}")

    def check_time_range(self):
        if not self.manual_stop and self.auto_mode and not self.collection_active:
            if self.is_within_time_range():
                self.start_collection()
        elif self.auto_mode and self.collection_active:
            if not self.is_within_time_range():
                self.stop_collection()

    def get_last_activity_id(self):
        """MSSQL에서 최대 cowactivity_id 조회"""
        try:
            with self.ms_engine.connect() as conn:
                query = text("SELECT ISNULL(MAX(cowactivity_id), 0) as max_id FROM ICT_ACTIVITY_LOG")
                result = conn.execute(query)
                max_id = result.scalar()
                return max_id
        except Exception as e:
            self.log_message(f"최대 ID 조회 오류: {str(e)}")
            raise  # 예외를 다시 발생시켜 상위에서 처리하도록 함

    def collect_data(self):
        try:
            # MSSQL에서 마지막 activity_id 조회
            try:
                last_activity_id = self.get_last_activity_id()
            except Exception:
                # 이미 get_last_activity_id에서 에러 메시지를 출력했으므로
                # 여기서는 조용히 처리 종료
                self.handle_error()
                return

            self.log_message(f"현재 MSSQL 마지막 ID: {last_activity_id}")

            # PostgreSQL에서 데이터 조회
            query = text("""
                SELECT a.cowactivity_id, a.cow_id, b.cow_number, b.cow_name,
                       a.counts, a.counts_perhr, a.cow_activity,
                       to_char(a.tstamp, 'YYYYMMDD') AS ymd,
                       to_char(a.tstamp, 'HH24MISS') AS hms
                FROM tblcowactivities a
                INNER JOIN tblcows b ON a.cow_id = b.cow_id
                WHERE a.cowactivity_id > :max_activity_id
                ORDER BY a.cow_id, cowactivity_id
            """)
            try:
                with self.pg_engine.connect() as conn:
                    result = conn.execute(query, {"max_activity_id": last_activity_id})
                    rows = result.fetchall()

                    if not rows:
                        self.log_message("PostgreSQL 신규 데이터 없음")
                        self.progress_bar.setValue(0)
                        return

                    total_rows = len(rows)
                    self.log_message(f"PostgreSQL 신규 데이터 조회: {total_rows}건")

                    # MSSQL에 데이터 저장
                    with self.ms_engine.connect() as ms_conn:
                        # 트랜잭션 시작
                        trans = ms_conn.begin()
                        try:
                            for i, row in enumerate(rows):
                                # SP 실행
                                sp_query = text("""
                                    EXEC P_ICT_ACTIVITY_LOG_M 
                                    @cowactivity_id=:cowactivity_id,
                                    @cow_id=:cow_id,
                                    @cow_number=:cow_number,
                                    @cow_name=:cow_name,
                                    @counts=:counts,
                                    @counts_perhr=:counts_perhr,
                                    @cow_activity=:cow_activity,
                                    @ymd=:ymd,
                                    @hms=:hms
                                """)

                                ms_conn.execute(sp_query, {
                                    "cowactivity_id": row.cowactivity_id,
                                    "cow_id": row.cow_id,
                                    "cow_number": row.cow_number,
                                    "cow_name": row.cow_name,
                                    "counts": row.counts,
                                    "counts_perhr": row.counts_perhr,
                                    "cow_activity": row.cow_activity,
                                    "ymd": row.ymd,
                                    "hms": row.hms
                                })

                                # 진행률 업데이트
                                progress = int((i + 1) / total_rows * 100)
                                self.progress_bar.setValue(progress)

                            # 트랜잭션 커밋
                            trans.commit()
                            self.log_message(f"MSSQL 데이터 저장 완료: {total_rows}건")
                        except Exception as e:
                            trans.rollback()
                            raise e
            except Exception as e:
                self.log_message(f"오류 발생: {str(e)}")
                self.handle_error()
                return

        except Exception as e:
            self.log_message(f"예기치 않은 오류 발생: {str(e)}")
            self.handle_error()
            return

    def handle_error(self):
        """오류 처리를 위한 통합 메서드"""
        self.progress_bar.setValue(0)
        if not self.auto_mode:  # 수동 모드일 경우에만 중지
            self.stop_collection()

    def is_within_time_range(self):
        current_time = QTime.currentTime()

        # 오전 범위 확인
        morning_start = QTime(int(self.morning_start_hour.text()),
                              int(self.morning_start_min.text()))
        morning_end = QTime(int(self.morning_end_hour.text()),
                            int(self.morning_end_min.text()))

        # 오후 범위 확인
        afternoon_start = QTime(int(self.afternoon_start_hour.text()),
                                int(self.afternoon_start_min.text()))
        afternoon_end = QTime(int(self.afternoon_end_hour.text()),
                              int(self.afternoon_end_min.text()))

        return ((morning_start <= current_time <= morning_end) or
                (afternoon_start <= current_time <= afternoon_end))

    def update_input_states(self, enabled: bool):
        """입력 필드들의 활성화/비활성화 상태 관리"""
        # 시간 입력 필드들
        self.morning_start_hour.setEnabled(enabled)
        self.morning_start_min.setEnabled(enabled)
        self.morning_end_hour.setEnabled(enabled)
        self.morning_end_min.setEnabled(enabled)
        self.afternoon_start_hour.setEnabled(enabled)
        self.afternoon_start_min.setEnabled(enabled)
        self.afternoon_end_hour.setEnabled(enabled)
        self.afternoon_end_min.setEnabled(enabled)
        # 수집주기 입력 필드
        self.interval_input.setEnabled(enabled)

    def start_collection(self):
        interval = int(self.interval_input.text()) * 1000  # 초를 밀리초로 변환
        self.collection_timer.start(interval)
        self.collection_active = True
        self.update_button_states()
        # 데이터 수집 시작 시 입력 필드 비활성화
        self.update_input_states(False)
        self.update_tray_tooltip()  # 상태 업데이트
        self.log_message("데이터 수집 시작")

    def stop_collection(self):
        self.collection_timer.stop()
        self.collection_active = False
        self.manual_stop = True
        self.update_button_states()
        # 데이터 수집 중지 시 입력 필드 활성화
        self.update_input_states(True)
        self.update_tray_tooltip()  # 상태 업데이트
        self.log_message("데이터 수집 중지")

    def manual_start_collection(self):
        self.manual_stop = False  # 수동 시작 시 중지 플래그 해제
        self.auto_mode = False
        if not self.is_within_time_range():
            # 범위 외 수동 시작: 1회 수집 후 중지
            self.collection_active = True  # 상태 변경
            self.update_button_states()  # 버튼 상태 업데이트
            self.collect_data()
            self.stop_collection()
        else:
            self.start_collection()

    def closeEvent(self, event):
        if not self.is_quitting:
            event.ignore()
            self.hide()
            self.tray_icon.showMessage(
                "보우메틱 활동량",
                "프로그램이 트레이로 최소화되었습니다.",
                QSystemTrayIcon.Information,
                2000
            )
        else:
            event.accept()

    def show_window(self):
        self.show()
        self.activateWindow()

    def quit_application(self):
        self.is_quitting = True
        # 실행 중인 타이머 정지
        if self.collection_active:
            self.stop_collection()
        # 트레이 아이콘 제거
        self.tray_icon.setVisible(False)
        # 애플리케이션 종료
        QApplication.quit()

    def tray_icon_activated(self, reason):
        if reason == QSystemTrayIcon.DoubleClick:
            self.show_window()

    # 기존 코드에 추가: 현재 상태 표시를 위한 메서드
    def update_tray_tooltip(self):
        if self.collection_active:
            self.tray_icon.setToolTip("보우메틱 활동량\n데이터 수집 중")
        else:
            self.tray_icon.setToolTip("보우메틱 활동량\n대기 중")

def main():
    app = QApplication(sys.argv)
    window = DataCollectorApp()
    window.show()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
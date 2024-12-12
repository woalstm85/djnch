import sys
import os
import logging
import json
import psutil
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout,
                             QHBoxLayout, QPushButton, QLabel, QLineEdit,
                             QProgressBar, QTextEdit, QMessageBox, QFrame, QGroupBox,
                             QSystemTrayIcon, QMenu, QAction)
from PyQt5.QtCore import QTimer, QTime
from PyQt5.QtGui import QIcon, QIntValidator
from PyQt5 import QtCore
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

class DataCollectorApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle('보우메틱 착유량')
        self.setGeometry(100, 100, 800, 700)  # 높이를 조금 줄임

        self.setWindowIcon(QIcon('milking.ico'))  # 타이틀바 아이콘 설정

        # 메뉴바 추가
        self.create_menu_bar()

        # 필수 변수 초기화
        self.collection_active = False
        self.auto_mode = True
        self.manual_stop = False
        self.is_quitting = False

        # 트레이 아이콘 설정
        self.tray_icon = QSystemTrayIcon(self)
        self.tray_icon.setIcon(QIcon('milking.ico'))

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
        self.config_file = 'config.json'
        self.load_config()

        # 타이머 초기화
        self.check_timer = QTimer()
        self.check_timer.timeout.connect(self.check_time_range)
        self.check_timer.start(1000)  # 1초마다 시간 체크

        self.collection_timer = QTimer()
        self.collection_timer.timeout.connect(self.collect_data)

        # 데이터베이스 연결 설정
        #self.pg_engine = create_engine("postgresql://postgres:1234@localhost:5432/tempdb")
        self.pg_engine = create_engine("postgresql://postgres:@localhost:5432/pvnc")
        self.ms_engine = create_engine("mssql+pyodbc://sa:ghltktjqj7%29@221.139.49.70:2433/DJNCH?driver=SQL+Server")

        # 설정 파일 로드
        self.load_config()

        # 로깅 설정
        self.setup_logger()
        self.current_log_date = datetime.now().date()

        # UI 설정
        self.setup_ui()

    def setup_logger(self):
        """로거 설정"""
        # logs 디렉토리 생성
        if not os.path.exists('logs'):
            os.makedirs('logs')

        # 현재 날짜로 로그 파일명 생성
        log_file = os.path.join('logs', f'milking_{datetime.now().strftime("%Y%m%d")}.log')

        # 로거 설정
        self.logger = logging.getLogger('MilkingLogger')
        self.logger.setLevel(logging.INFO)

        # 핸들러 설정
        handler = logging.FileHandler(log_file, encoding='utf-8')
        handler.setFormatter(logging.Formatter(
            '%(asctime)s [%(levelname)s] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ))

        # 기존 핸들러 제거 후 새로운 핸들러 추가
        self.logger.handlers.clear()
        self.logger.addHandler(handler)

    def check_log_date(self):
        #날짜가 변경되었는지 확인하고 필요시 로거 재설정
        current_date = datetime.now().date()
        if current_date != self.current_log_date:
            self.current_log_date = current_date
            self.setup_logger()
            # 날짜가 변경될 때 오래된 로그 정리
            self.cleanup_old_logs()

    def cleanup_old_logs(self):
        #10일 이상 지난 로그 파일 삭제
        try:
            # 오늘 날짜 계산
            today = datetime.now()
            cutoff_date = today - timedelta(days=10)

            # logs 디렉토리 내의 파일 검사
            log_dir = 'logs'
            if os.path.exists(log_dir):
                for filename in os.listdir(log_dir):
                    if filename.startswith('milking_') and filename.endswith('.log'):
                        try:
                            # 파일명에서 날짜 추출 (milking_20241121.log -> 20241121)
                            file_date_str = filename.replace('milking_', '').replace('.log', '')
                            file_date = datetime.strptime(file_date_str, '%Y%m%d')

                            # 10일 이상 지난 파일 삭제
                            if file_date.date() < cutoff_date.date():
                                file_path = os.path.join(log_dir, filename)
                                os.remove(file_path)
                                print(f"Removed old log file: {filename}")  # 디버깅용
                        except ValueError:
                            # 파일명 형식이 잘못된 경우 무시
                            continue
        except Exception as e:
            print(f"Log cleanup error: {str(e)}")  # 디버깅용

    def create_menu_bar(self):
        """메뉴바 생성"""
        menubar = self.menuBar()
        menubar.setStyleSheet("""
            QMenuBar {
                background-color: #f5f5f5;
                border-bottom: 1px solid #3F3F3F;
            }
            QMenuBar::item {
                padding: 4px 10px;
                background-color: transparent;
            }
            QMenuBar::item:selected {
                background-color: #e0e0e0;
            }
            QMenu {
                background-color: #ffffff;
                border: 1px solid #3F3F3F;
            }
            QMenu::item {
                padding: 4px 20px;
            }
            QMenu::item:selected {
                background-color: #e0e0e0;
            }
        """)

        # 파일 메뉴
        file_menu = menubar.addMenu('파일')

        # 종료 액션
        exit_action = QAction('종료', self)
        exit_action.setStatusTip('프로그램 종료')
        exit_action.triggered.connect(self.quit_application)

        # 메뉴에 액션 추가
        file_menu.addAction(exit_action)

    def check_interval_setting(self):
        """수집 주기 설정값 변경 체크"""
        new_interval = self.get_collection_interval()
        if new_interval != self.current_interval:
            self.log_message(f"수집 주기가 변경되었습니다: {self.current_interval}초 -> {new_interval}초")
            self.current_interval = new_interval

            # 수집 중이면 타이머 재시작
            if self.collection_active:
                self.collection_timer.stop()
                self.collection_timer.start(self.current_interval * 1000)

            # UI 업데이트
            self.interval_input.setText(str(self.current_interval))

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

        # QLineEdit에 입력 제한 추가
        validator_hour = QIntValidator(0, 23)  # 시간은 0-23
        validator_min = QIntValidator(0, 59)  # 분은 0-59
        validator_interval = QIntValidator(1, 3600)  # 수집주기는 1-3600

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


        # 수집주기 입력필드 생성 및 설정
        initial_interval = self.get_collection_interval()  # DB에서 초기값 가져오기
        self.interval_input = QLineEdit(str(initial_interval))
        self.interval_input.setFixedWidth(100)
        self.interval_input.setFixedHeight(40)
        self.interval_input.setAlignment(QtCore.Qt.AlignCenter)
        self.interval_input.setReadOnly(True)  # 읽기 전용으로 설정
        interval_layout.addWidget(self.interval_input)
        interval_layout.addSpacing(20)  # 수집주기 input 뒤에 여백 추가

        # 수집주기 체크 타이머 설정
        self.interval_check_timer = QTimer()
        self.interval_check_timer.timeout.connect(self.check_db_interval)
        self.interval_check_timer.start(5000)  # 5초마다 체크

        # 현재 적용된 수집 주기 저장
        self.current_interval = initial_interval

        # 시간 입력 제한
        self.morning_start_hour.setValidator(validator_hour)
        self.morning_start_min.setValidator(validator_min)
        self.morning_end_hour.setValidator(validator_hour)
        self.morning_end_min.setValidator(validator_min)
        self.afternoon_start_hour.setValidator(validator_hour)
        self.afternoon_start_min.setValidator(validator_min)
        self.afternoon_end_hour.setValidator(validator_hour)
        self.afternoon_end_min.setValidator(validator_min)

        # 수집주기 입력 제한
        self.interval_input.setValidator(validator_interval)


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

        # 상태바 설정
        self.statusBar = self.statusBar()
        self.statusBar.setStyleSheet("""
            QStatusBar {
                border-top: 1px solid #3F3F3F;
                background: #f5f5f5;
            }
        """)

        # 상태바 레이블 생성
        self.memory_label = QLabel()
        self.time_label = QLabel()
        self.status_label = QLabel()

        # 상태바에 위젯 추가
        self.statusBar.addPermanentWidget(self.status_label)
        self.statusBar.addPermanentWidget(self.memory_label)
        self.statusBar.addPermanentWidget(self.time_label)

        # 상태바 업데이트 타이머
        self.status_timer = QTimer()
        self.status_timer.timeout.connect(self.update_status_bar)
        self.status_timer.start(1000)  # 1초마다 업데이트

    def check_db_interval(self):
        """DB의 수집주기 설정값 확인 및 업데이트"""
        try:
            new_interval = self.get_collection_interval()
            current_interval = int(self.interval_input.text())

            if new_interval != current_interval:
                # 자동 업데이트 플래그 설정
                self.interval_input.blockSignals(True)  # 시그널 임시 차단
                self.interval_input.setText(str(new_interval))
                self.interval_input.blockSignals(False)  # 시그널 복원

                if self.collection_active:
                    self.collection_timer.stop()
                    self.collection_timer.start(new_interval * 1000)

                self.log_message(f"수집주기가 {current_interval}초에서 {new_interval}초로 변경되었습니다.")

        except Exception as e:
            self.log_message(f"수집주기 확인 중 오류 발생: {str(e)}")

    def update_status_bar(self):
        """상태바 정보 업데이트"""
        try:
            # 메모리 사용량
            process = psutil.Process()
            memory_info = process.memory_info()
            memory_usage = memory_info.rss / 1024 / 1024  # MB 단위로 변환
            self.memory_label.setText(f"메모리 사용량: {memory_usage:.1f} MB | ")

            # 현재 시간
            current_time = QTime.currentTime().toString("HH:mm:ss")
            self.time_label.setText(f"현재 시간: {current_time}")

            # 현재 상태
            if self.collection_active:
                self.status_label.setText("상태: 데이터 수집 중 | ")
            else:
                self.status_label.setText("상태: 대기 중 | ")

        except Exception as e:
            print(f"상태바 업데이트 오류: {str(e)}")

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
        #로그 메시지 추가 및 최신 500줄 유지
        current_time = QTime.currentTime().toString("HH:mm:ss")
        self.log_text.append(f"[{current_time}] {message}")

        # 로그 라인 수 체크
        doc = self.log_text.document()
        if doc.lineCount() > 20:
            # 커서를 처음으로 이동
            cursor = self.log_text.textCursor()
            cursor.movePosition(cursor.Start)
            # 첫 줄 선택
            cursor.movePosition(cursor.Down, cursor.KeepAnchor,
                                doc.lineCount() - 20)
            # 선택된 텍스트 삭제 (오래된 로그 제거)
            cursor.removeSelectedText()
            cursor.removeSelectedText()  # 남은 빈 줄 제거

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
        except FileNotFoundError:
            # 기본 설정 (interval 제외)
            self.config = {
                'morning_start': {'hour': '06', 'minute': '00'},
                'morning_end': {'hour': '10', 'minute': '00'},
                'afternoon_start': {'hour': '15', 'minute': '00'},
                'afternoon_end': {'hour': '19', 'minute': '00'}
            }
            self.save_config()

    def save_config(self):
        config = {
            'morning_start': {'hour': self.morning_start_hour.text(), 'minute': self.morning_start_min.text()},
            'morning_end': {'hour': self.morning_end_hour.text(), 'minute': self.morning_end_min.text()},
            'afternoon_start': {'hour': self.afternoon_start_hour.text(), 'minute': self.afternoon_start_min.text()},
            'afternoon_end': {'hour': self.afternoon_end_hour.text(), 'minute': self.afternoon_end_min.text()}
        }
        with open(self.config_file, 'w') as f:
            json.dump(config, f, indent=4)

    def get_collection_interval(self):
        """MSSQL에서 수집 주기 값을 조회"""
        try:
            with self.ms_engine.connect() as conn:
                query = text("""
                    SELECT ETC_TEXT1 AS TIM
                    FROM CODE_INFO_DTL
                    WHERE SYS_KIND = 'CONTROL'
                    AND CODE_KIND = 'SYS142'
                """)
                result = conn.execute(query)
                interval = result.scalar()
                if interval:
                    return int(interval)
                return 1800  # 기본값
        except Exception as e:
            self.log_message(f"수집 주기 조회 오류: {str(e)}")
            return 1800  # 오류 시 기본값

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
        if not self.manual_stop and self.auto_mode:  # collection_active 체크 제거
            if self.is_within_time_range():
                if not self.collection_active:  # 수집이 실행중이 아닐때만 시작
                    self.start_collection()
            else:
                if self.collection_active:  # 수집이 실행중일때만 중지
                    self.stop_collection()

    def get_last_tstamp(self):
        """MSSQL에서 최대 cowactivity_id 조회"""
        try:
            with self.ms_engine.connect() as conn:
                query = text("SELECT ISNULL(MAX(tstamp), CAST('1900-01-01 00:00:00.000000' AS DATETIME2(6)))  FROM TEST_ICT_MILKING_LOG WITH(NOLOCK)")
                result = conn.execute(query)
                max_tstamp = result.scalar()
                return max_tstamp
        except Exception as e:
            self.log_message(f"최대 ID 조회 오류: {str(e)}")
            raise  # 예외를 다시 발생시켜 상위에서 처리하도록 함

    def collect_data(self):
        # 날짜 체크 및 로거 업데이트
        self.check_log_date()

        try:
            # MSSQL에서 마지막 activity_id 조회
            try:
                last_tstamp = self.get_last_tstamp()
                msg = f"현재 MSSQL 마지막 ID: {last_tstamp}"
                self.log_message(msg)
            except Exception as e:
                error_msg = f"최대 ID 조회 오류: {str(e)}"
                self.log_message(error_msg)
                self.handle_error()
                return

            # PostgreSQL에서 데이터 조회
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

            try:
                with self.pg_engine.connect() as conn:
                    result = conn.execute(query, {"max_tstamp": last_tstamp})
                    rows = result.fetchall()

                    if not rows:
                        msg = f"PostgreSQL 신규 데이터 없음\n{'*' * 50}"
                        self.log_message(msg)
                        self.progress_bar.setValue(0)
                        return

                    total_rows = len(rows)
                    msg = f"PostgreSQL 신규 데이터 조회: {total_rows}건"
                    self.log_message(msg)
                    self.logger.info(msg)

                    # MSSQL에 데이터 저장
                    with self.ms_engine.connect() as ms_conn:
                        # 트랜잭션 시작
                        trans = ms_conn.begin()
                        try:
                            for i, row in enumerate(rows):
                                # SP 실행
                                sp_query = text("""
                                EXEC P_TEST_ICT_MILKING_LOG_M 
                                    @milking_id=:milking_id,
                                    @ymd=:ymd,
                                    @am_pm=:am_pm,
                                    @hms=:hms,
                                    @cow_id=:cow_id,
                                    @cow_number=:cow_number,
                                    @cow_name=:cow_name,
                                    @milkingshift_id=:milkingshift_id,
                                    @detacher_address=:detacher_address,
                                    @id_tag_number_assigned=:id_tag_number_assigned,
                                    @milk_weight=:milk_weight,
                                    @dumped_milk=:dumped_milk,
                                    @milk_conductivity=:milk_conductivity,
                                    @cow_activity=:cow_activity,
                                    @flow_0_15_sec=:flow_0_15_sec,
                                    @flow_15_30_sec=:flow_15_30_sec,
                                    @flow_30_60_sec=:flow_30_60_sec,
                                    @flow_60_120_sec=:flow_60_120_sec,
                                    @time_in_low_flow=:time_in_low_flow,
                                    @reattach_counter=:reattach_counter,
                                    @percent_expected_milk=:percent_expected_milk,
                                    @tstamp=:tstamp_string
                                """)

                                ms_conn.execute(sp_query, {
                                    "milking_id": row.milking_id,
                                    "ymd": row.ymd,
                                    "am_pm": row.am_pm,
                                    "hms": row.hms,
                                    "cow_id": row.cow_id,
                                    "cow_number": row.cow_number,
                                    "cow_name": row.cow_name,
                                    "milkingshift_id": row.milkingshift_id,
                                    "detacher_address": row.detacher_address,
                                    "id_tag_number_assigned": row.id_tag_number_assigned,
                                    "milk_weight": row.milk_weight,
                                    "dumped_milk": row.dumped_milk,
                                    "milk_conductivity": row.milk_conductivity,
                                    "cow_activity": row.cow_activity,
                                    "flow_0_15_sec": row.flow_0_15_sec,
                                    "flow_15_30_sec": row.flow_15_30_sec,
                                    "flow_30_60_sec": row.flow_30_60_sec,
                                    "flow_60_120_sec": row.flow_60_120_sec,
                                    "time_in_low_flow": row.time_in_low_flow,
                                    "reattach_counter": row.reattach_counter,
                                    "percent_expected_milk": row.percent_expected_milk,
                                    "tstamp_string": row.tstamp_string
                                })

                                # 진행률 업데이트
                                progress = int((i + 1) / total_rows * 100)
                                self.progress_bar.setValue(progress)

                            # 트랜잭션 커밋
                            trans.commit()
                            msg = f"MSSQL 데이터 저장 완료: {total_rows}건\n{'*' * 50}"
                            self.log_message(msg)
                            self.logger.info(msg)

                        except Exception as e:
                            trans.rollback()
                            error_msg = f"MSSQL 데이터 저장 실패: {str(e)}"
                            self.log_message(error_msg)
                            self.logger.error(error_msg)
                            raise e

            except Exception as e:
                error_msg = f"데이터베이스 처리 오류: {str(e)}"
                self.log_message(error_msg)
                self.logger.error(error_msg)
                self.handle_error()
                return

        except Exception as e:
            error_msg = f"예기치 않은 오류 발생: {str(e)}"
            self.log_message(error_msg)
            self.logger.error(error_msg)
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
        # self.interval_input.setEnabled(enabled)

    def start_collection(self):
        interval = self.get_collection_interval() * 1000  # DB에서 가져온 값 사용
        self.current_interval = interval // 1000  # 현재 값 저장
        self.collection_timer.start(interval)
        self.collection_active = True
        self.update_button_states()
        self.update_input_states(False)
        self.update_tray_tooltip()
        self.log_message(f"데이터 수집 시작 (수집 주기: {self.current_interval}초)")

    def stop_collection(self):
        """데이터 수집 중지"""
        try:
            self.collection_timer.stop()
            self.collection_active = False
            self.manual_stop = False  # 중지 후 manual_stop 플래그 리셋
            self.auto_mode = True  # auto_mode 복원

            # 진행 중인 DB 트랜잭션이 있다면 롤백
            try:
                if hasattr(self, 'current_transaction'):
                    self.current_transaction.rollback()
            except:
                pass

            self.update_button_states()
            self.update_input_states(True)
            self.update_tray_tooltip()
            self.log_message("데이터 수집이 중지되었습니다.")

        except Exception as e:
            self.log_message(f"수집 중지 중 오류 발생: {str(e)}")
            self.logger.error(f"수집 중지 중 오류 발생: {str(e)}")

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
                "보우메틱 착유량",
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
        if self.collection_active:
            reply = QMessageBox.question(
                self,
                '종료 확인',
                "데이터 수집이 진행 중입니다.\n안전하게 중지하고 종료하시겠습니까?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )

            if reply == QMessageBox.Yes:
                try:
                    # 진행 중인 작업 안전하게 중지
                    self.log_message("프로그램 종료 요청 - 데이터 수집 중지 중...")
                    self.logger.info("프로그램 종료 요청 - 데이터 수집 중지 시작")

                    # 수집 중지
                    self.stop_collection()

                    # DB 연결 정리
                    if hasattr(self, 'pg_engine'):
                        self.pg_engine.dispose()
                    if hasattr(self, 'ms_engine'):
                        self.ms_engine.dispose()

                    self.log_message("모든 작업이 안전하게 중지되었습니다.")
                    self.logger.info("프로그램 정상 종료")

                    # 종료 처리
                    self.is_quitting = True
                    self.tray_icon.setVisible(False)
                    QApplication.quit()
                except Exception as e:
                    QMessageBox.critical(
                        self,
                        '오류',
                        f"종료 처리 중 오류가 발생했습니다:\n{str(e)}\n강제 종료됩니다.",
                        QMessageBox.Ok
                    )
                    self.is_quitting = True
                    self.tray_icon.setVisible(False)
                    QApplication.quit()
            else:
                # 사용자가 종료를 취소함
                return
        else:
            # 수집이 진행 중이 아닐 때는 바로 종료
            self.is_quitting = True
            self.tray_icon.setVisible(False)
            QApplication.quit()

    def closeEvent(self, event):
        """윈도우 X 버튼 클릭 시 호출되는 이벤트"""
        if not self.is_quitting:
            if self.collection_active:
                reply = QMessageBox.question(
                    self,
                    '최소화 확인',
                    "데이터 수집이 진행 중입니다.\n트레이로 최소화하시겠습니까?\n\n(종료하시려면 '파일 > 종료' 메뉴를 사용해주세요)",
                    QMessageBox.Yes | QMessageBox.No,
                    QMessageBox.Yes
                )

                if reply == QMessageBox.Yes:
                    event.ignore()
                    self.hide()
                    self.tray_icon.showMessage(
                        "보우메틱 착유량",
                        "프로그램이 트레이로 최소화되었습니다.\n데이터 수집은 계속 진행됩니다.",
                        QSystemTrayIcon.Information,
                        2000
                    )
                else:
                    event.ignore()
            else:
                event.ignore()
                self.hide()
                self.tray_icon.showMessage(
                    "보우메틱 착유량",
                    "프로그램이 트레이로 최소화되었습니다.",
                    QSystemTrayIcon.Information,
                    2000
                )
        else:
            event.accept()

    def tray_icon_activated(self, reason):
        if reason == QSystemTrayIcon.DoubleClick:
            self.show_window()

    # 기존 코드에 추가: 현재 상태 표시를 위한 메서드
    def update_tray_tooltip(self):
        if self.collection_active:
            self.tray_icon.setToolTip("보우메틱 착유량\n데이터 수집 중")
        else:
            self.tray_icon.setToolTip("보우메틱 착유량\n대기 중")

def main():
    app = QApplication(sys.argv)
    app.setWindowIcon(QIcon('milking.ico'))  # 애플리케이션 전체 아이콘 설정
    window = DataCollectorApp()
    window.show()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()





import sys
import os
import pandas as pd
from PyQt5.QtWidgets import QApplication, QTableWidget, QTableWidgetItem, QMainWindow, QVBoxLayout, QWidget, QLabel, \
    QHBoxLayout
from PyQt5.QtCore import Qt, QTimer
from PyQt5.QtGui import QColor, QBrush, QFont
from PyQt5.QtWebEngineWidgets import QWebEngineView
from PyQt5.QtWidgets import QHeaderView
from sqlalchemy import create_engine, text
import plotly.graph_objects as go
# 현재 스크립트의 디렉토리를 기준으로 경로 설정
base_path = os.path.dirname(os.path.abspath(__file__))
# QtWebEngineProcess.exe 경로 설정
qt_process_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '_internal', 'PyQt5', 'Qt5', 'bin', 'QtWebEngineProcess.exe')

# MSSQL 데이터베이스 연결 설정
mssql_connection_string = "mssql+pyodbc://sa:ghltktjqj7%29@221.139.49.70:2433/DJNCH?driver=SQL+Server"
mssql_engine = create_engine(mssql_connection_string, fast_executemany=True)

# SP 호출
def fetch_data(param1, param2):
    with mssql_engine.connect() as connection:
        result = connection.execute(text("EXEC P_DASHBOARD_V1 :param1, :param2"), {'param1': param1, 'param2': param2})
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        if param1 == 'V2':
            df['YMD'] = pd.to_datetime(df['YMD'], format='%Y%m%d')
    return df

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Cattle Data Viewer")
        self.resize(1200, 800)

        # 메인 레이아웃 설정
        main_widget = QWidget()
        main_layout = QVBoxLayout(main_widget)

        # 좌우 레이아웃 생성 및 추가
        table_layout = QHBoxLayout()
        table_layout.addLayout(self.create_left_layout(), stretch=4)
        table_layout.addLayout(self.create_right_layout(), stretch=4)
        main_layout.addLayout(table_layout)

        # 각 QTableWidget에 에러 메시지를 덮는 레이어 추가
        self.v1_error_overlay = self.create_error_overlay(self.v1_table)
        self.v3_error_overlay = self.create_error_overlay(self.v3_table)
        self.v6_error_overlay = self.create_error_overlay(self.v6_table)

        # 초기에는 에러 메시지 레이어를 숨깁니다
        self.hide_error_overlay(self.v1_error_overlay)
        self.hide_error_overlay(self.v3_error_overlay)
        self.hide_error_overlay(self.v6_error_overlay)

        # 메인 위젯 설정
        self.setCentralWidget(main_widget)

        # 각 데이터의 이전 상태를 저장할 변수 초기화
        self.v1_data = None
        self.v2_data = None
        self.v3_data = None
        self.v4_data = None
        self.v5_data = None
        self.v6_data = None
        self.v7_data = None

        # 자동 갱신 타이머 설정
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.update_tables_and_charts)
        self.timer.start(30000)  # 30초마다 새로 고침
        self.update_tables_and_charts()

    def update_tables_and_charts(self):  # data load
        # 각 데이터 로드를 fetch_data_with_error_handling으로 관리
        self.v1_data_table(self.fetch_data_with_error_handling('V1', 'M2', self.v1_table))
        self.v2_data_chart(self.fetch_data_with_error_handling('V2', 'M2', self.v2_chart_view))
        self.v3_data_table(self.fetch_data_with_error_handling('V3', 'M2', self.v3_table))
        self.v4_data_chart(self.fetch_data_with_error_handling('V4', 'M2', self.v4_chart_view))
        self.v5_data_chart(self.fetch_data_with_error_handling('V5', 'M2', self.v5_chart_view))
        self.v6_data_table(self.fetch_data_with_error_handling('V6', 'M2', self.v6_table))
        self.v7_data_chart(self.fetch_data_with_error_handling('V7', 'M2', self.v7_chart_view))

    def create_left_layout(self): # 왼쪽 레이아웃 생성 (우사별 사육두수, 차트 등)
        left_layout = QVBoxLayout()
        # v1 우사별 사육두수 타이틀
        v1_title_layout = QHBoxLayout()
        v1_left_title_label = QLabel("  우사별 사육두수")
        v1_left_title_label.setFont(QFont("Arial", 12, QFont.Bold))
        v1_left_title_label.setStyleSheet("color: #763500;")
        # 높이와 너비 고정 (필요에 따라 조정 가능)
        v1_left_title_label.setFixedHeight(20)  # 원하는 높이로 조정

        v1_title_layout.addWidget(v1_left_title_label, alignment=Qt.AlignLeft)

        left_layout.addLayout(v1_title_layout)

        self.v1_table = QTableWidget(0, 14, self)
        self.v1_table.setStyleSheet("background-color: #111111; color: white; gridline-color: #5C5C5C;")
        self.v1_table.verticalHeader().setVisible(False)
        self.v1_table.horizontalHeader().setVisible(False)
        self.v1_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.v1_headers()
        left_layout.addWidget(self.v1_table)

        # V2 차트 일별 착유현황 추이
        self.v2_chart_view = QWebEngineView()
        left_layout.addWidget(self.v2_chart_view)

        # V4 차트 우군별 비육일수별 산유량 분포
        self.v4_chart_view = QWebEngineView()
        left_layout.addWidget(self.v4_chart_view)

        return left_layout

    def create_right_layout(self):  # 오른쪽 레이아웃 생성 (사육현황, 차트 등)
        right_layout = QVBoxLayout()

        # v3 축주별 사육현황 타이틀
        v3_right_title_label = QLabel("  축주별 사육현황")
        v3_right_title_label.setFont(QFont("Arial", 12, QFont.Bold))
        v3_right_title_label.setStyleSheet("color: #763500;")
        # 높이와 너비 고정 (필요에 따라 조정 가능)
        v3_right_title_label.setFixedHeight(20)

        right_layout.addWidget(v3_right_title_label)

        # v3 축주별 사육현황 테이블
        self.v3_table = QTableWidget(0, 13, self)
        self.v3_table.setStyleSheet("background-color: #111111; color: white; gridline-color: #5C5C5C;")
        self.v3_table.verticalHeader().setVisible(False)
        self.v3_table.horizontalHeader().setVisible(False)
        self.v3_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.v3_header()
        right_layout.addWidget(self.v3_table)

        # v5 산차별 구성비 차트
        self.v5_chart_view = QWebEngineView()
        right_layout.addWidget(self.v5_chart_view)

        # v6 두당 산유량 분포 표 라벨
        v6_table_layout = QVBoxLayout()
        v6_title_label = QLabel("두당 산유량 분포")
        v6_title_label.setFont(QFont("Arial", 12, QFont.Bold))
        v6_title_label.setStyleSheet("color: #763500;")
        v6_table_layout.addWidget(v6_title_label, alignment=Qt.AlignLeft)

        # v6 두당 산유량 분포 표 테이블
        self.v6_table = QTableWidget(0, 8, self)
        self.v6_table.setStyleSheet("background-color: #111111; color: white; gridline-color: #5C5C5C;")
        self.v6_table.verticalHeader().setVisible(False)
        self.v6_table.horizontalHeader().setVisible(False)
        self.v6_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.v6_table_header()
        v6_table_layout.addWidget(self.v6_table)
        right_layout.addLayout(v6_table_layout)

        # v7 두당 산유량 구성비 차트
        self.v7_chart_view = QWebEngineView()
        right_layout.addWidget(self.v7_chart_view)

        return right_layout

    """"""""""""""""""""""""""""""""" 
    start-v1 : 우사별 사육 두수 (테이블)
    """""""""""""""""""""""""""""""""
    def v1_headers(self):   # v1 table header setting
        self.v1_table.setRowCount(2)

        # 그룹 헤더 및 하위 헤더 생성
        self.v1_table.setSpan(0, 0, 2, 2)
        header_item = QTableWidgetItem("구분")
        header_item.setFont(QFont("Arial", weight=QFont.Bold))
        header_item.setBackground(QBrush(QColor("#4B4B4B")))
        self.v1_table.setItem(0, 0, header_item)

        self.v1_table.setSpan(0, 2, 2, 1)
        header_item_sum = QTableWidgetItem("합계")
        header_item_sum.setFont(QFont("Arial", weight=QFont.Bold))
        header_item_sum.setBackground(QBrush(QColor("#4B4B4B")))
        self.v1_table.setItem(0, 2, header_item_sum)

        # 각 동 헤더 생성
        header_info = {
            "1동": (3, 4),
            "2동": (5, 6),
            "3동": (7, 8),
            "4동": (9, 10),
            "5동": (11, 12)
        }

        for text, (start_column, end_column) in header_info.items():
            self.v1_main_header(text, start_column, end_column)     # v1_main_header main header 설정

        # 6동 헤더 (단일 열)
        self.v1_table.setSpan(0, 13, 2, 1)
        header_item_6dong = QTableWidgetItem("6동")
        header_item_6dong.setFont(QFont("Arial", weight=QFont.Bold))
        header_item_6dong.setBackground(QBrush(QColor("#4B4B4B")))
        self.v1_table.setItem(0, 13, header_item_6dong)

        # v1 sub title 설정
        sub_headers = [
            (2, "합계"),
            (3, "1A"), (4, "1B"),
            (5, "2A"), (6, "2B"),
            (7, "3A"), (8, "3B"),
            (9, "4A"), (10, "4B"),
            (11, "5A"), (12, "5B")
        ]

        for column, text in sub_headers:
            self.v1_sub_header(column, text)

    def v1_main_header(self, text, start_column, end_column):   # v1 table main header
        self.v1_table.setSpan(0, start_column, 1, end_column - start_column + 1)
        header_item = QTableWidgetItem(text)
        header_item.setFont(QFont("Arial", weight=QFont.Bold))
        header_item.setBackground(QBrush(QColor("#4B4B4B")))
        self.v1_table.setItem(0, start_column, header_item)

    def v1_sub_header(self, column, text):  # v1 table sub header
        sub_header_item = QTableWidgetItem(text)
        sub_header_item.setFont(QFont("Arial", 9, QFont.Bold))
        sub_header_item.setBackground(QBrush(QColor("#5C5C5C")))
        self.v1_table.setItem(1, column, sub_header_item)

    def v1_data_table(self, data):    # v1 table data setting
        df = data
        # 새 데이터가 이전 데이터와 같으면 업데이트 중단
        if df is None or df.equals(self.v1_data):
            return

        # 데이터가 변경된 경우에만 테이블 업데이트 수행
        self.v1_data = df  # 새로운 데이터를 이전 데이터로 저장

        start_row = 2
        self.v1_table.setRowCount(start_row + len(df))

        prev_row_index = None
        for idx, row in df.iterrows():
            row_index = start_row + idx
            if row['GROW_NM'] == "착유우":
                if prev_row_index is None:
                    prev_row_index = row_index
                    self.v1_table.setItem(prev_row_index, 0, QTableWidgetItem(row['GROW_NM']))
                self.v1_table.setSpan(prev_row_index, 0, row_index - prev_row_index + 1, 1)
                self.v1_table.setItem(row_index, 1, QTableWidgetItem(row['BIRTH_CNT']))
            else:
                self.v1_table.setSpan(row_index, 0, 1, 2)
                merged_item = QTableWidgetItem(f"{row['GROW_NM']} {row['BIRTH_CNT']}")
                self.v1_table.setItem(row_index, 0, merged_item)

            self.v1_table.setItem(row_index, 2, QTableWidgetItem(str(row['합계'])))
            self.v1_table.setItem(row_index, 3, QTableWidgetItem(str(row['1A'])))
            self.v1_table.setItem(row_index, 4, QTableWidgetItem(str(row['1B'])))
            self.v1_table.setItem(row_index, 5, QTableWidgetItem(str(row['2A'])))
            self.v1_table.setItem(row_index, 6, QTableWidgetItem(str(row['2B'])))
            self.v1_table.setItem(row_index, 7, QTableWidgetItem(str(row['3A'])))
            self.v1_table.setItem(row_index, 8, QTableWidgetItem(str(row['3B'])))
            self.v1_table.setItem(row_index, 9, QTableWidgetItem(str(row['4A'])))
            self.v1_table.setItem(row_index, 10, QTableWidgetItem(str(row['4B'])))
            self.v1_table.setItem(row_index, 11, QTableWidgetItem(str(row['5A'])))
            self.v1_table.setItem(row_index, 12, QTableWidgetItem(str(row['5B'])))
            self.v1_table.setItem(row_index, 13, QTableWidgetItem(str(row['6A'])))

            if row['GROW_NM'] == "합계":
                for col in range(self.v1_table.columnCount()):
                    item = self.v1_table.item(row_index, col)
                    if item:
                        item.setBackground(QBrush(QColor("#763500")))
                        item.setForeground(QBrush(QColor("white")))
                        item.setFont(QFont("Arial", weight=QFont.Bold))

        # 중앙 정렬 설정
        for i in range(self.v1_table.rowCount()):
            for j in range(self.v1_table.columnCount()):
                item = self.v1_table.item(i, j)
                if item:
                    item.setTextAlignment(Qt.AlignCenter)

        # 테이블의 높이를 행 수에 맞게 조절
        row_height = self.v1_table.verticalHeader().defaultSectionSize()
        self.v1_table.setFixedHeight(row_height * self.v1_table.rowCount() + self.v1_table.horizontalHeader().height() + 2)

    """"""""""""""""""""""""""""""""" 
    start-v2 : 일별 착유 현황 추이 (차트)
    """""""""""""""""""""""""""""""""

    def v2_data_chart(self, data):
        v2_chart_data = data

        # 데이터가 None이면 반환
        if v2_chart_data is None:
            self.v2_data = None
            return

        # 이전 데이터와 동일한 경우 업데이트 중단
        if self.v2_data is not None and v2_chart_data.equals(self.v2_data):
            return

        self.v2_data = v2_chart_data

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=v2_chart_data['YMD'],
            y=v2_chart_data['COW_CNT'],
            name='두수',
            marker_color="#1f77b4",
            textposition='none',  # 막대바 값 텍스트 제거
            yaxis='y1'
        ))
        fig.add_trace(go.Scatter(
            x=v2_chart_data['YMD'],
            y=v2_chart_data['MILK_QTY'],
            name='착유량',
            mode='lines+markers+text',
            marker_color="#ff7f0e",
            line=dict(color="#ff7f0e", width=2),
            text=[f"{x:,.2f}" for x in v2_chart_data['MILK_QTY']],
            textposition='top center',
            textfont=dict(color="black", size=15),
            yaxis='y2'
        ))
        fig.update_layout(
            title_text="일별 착유 현황 추이 그래프(2주간)",
            title_font_size=16,
            template="simple_white",
            plot_bgcolor="white",
            paper_bgcolor="white",
            xaxis=dict(
                tickformat="%m.%d",
                color="black",
                dtick="D1",
                tickfont=dict(size=14),
                gridcolor="lightgray"  # X축 가로선 색상 설정
            ),
            yaxis=dict(
                title="두수",
                color="black",
                dtick=100,
                titlefont=dict(size=14),
                tickfont=dict(size=12),
                gridcolor="lightgray"  # Y축 가로선 색상 설정
            ),
            yaxis2=dict(
                title="착유량",
                overlaying='y',
                side='right',
                color="black",
                showgrid=False,  # Y2축은 격자선을 표시하지 않음
                dtick=1000,
                titlefont=dict(size=14),
                tickfont=dict(size=12)
            ),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.1,
                xanchor="center",
                x=0.5,
                font=dict(size=14, color="black")
            )
        )

        # HTML 생성 및 업데이트
        self.v2_chart_view.setHtml(self.generate_html(fig.to_json()))

    """"""""""""""""""""""""""""""""" 
    start-v3 : 축주별 사육 현황 (테이블)  
    """""""""""""""""""""""""""""""""
    def v3_header(self):     # v3 table data setting
        self.v3_table.setRowCount(2)  # 타이틀 행을 위해 행 개수 설정
        self.v3_table.setColumnCount(12)  # 필요한 열 개수 설정

        # 첫 번째 타이틀 "축주"
        self.v3_table.setSpan(0, 0, 2, 1)  # 2행에 걸쳐 1열 병합
        title_item = QTableWidgetItem("축주")
        title_item.setFont(QFont("Arial", weight=QFont.Bold))
        title_item.setBackground(QBrush(QColor("#4B4B4B")))
        self.v3_table.setItem(0, 0, title_item)

        # "착유량" 헤더 및 하위 항목 설정
        self.v3_table.setSpan(0, 1, 1, 2)  # 3개의 열 병합
        milking_title = QTableWidgetItem("착유량")
        milking_title.setFont(QFont("Arial", weight=QFont.Bold))
        milking_title.setBackground(QBrush(QColor("#4B4B4B")))
        self.v3_table.setItem(0, 1, milking_title)

        # "산차별 착유 두수" 헤더 및 하위 항목 설정
        self.v3_table.setSpan(0, 3, 1, 6)  # 6개의 열 병합
        parity_title = QTableWidgetItem("산차별 착유 두수")
        parity_title.setFont(QFont("Arial", weight=QFont.Bold))
        parity_title.setBackground(QBrush(QColor("#4B4B4B")))
        self.v3_table.setItem(0, 3, parity_title)

        self.v3_table.setSpan(0, 9, 1, 1)  # 각 열을 개별 타이틀로 설정
        sub_header_choimansak = QTableWidgetItem("초임만삭")
        sub_header_choimansak.setFont(QFont("Arial", weight=QFont.Bold))
        sub_header_choimansak.setBackground(QBrush(QColor("#4B4B4B")))
        self.v3_table.setItem(0, 9, sub_header_choimansak)

        sub_header_gunyuu = QTableWidgetItem("건유우")
        sub_header_gunyuu.setFont(QFont("Arial", weight=QFont.Bold))
        sub_header_gunyuu.setBackground(QBrush(QColor("#4B4B4B")))
        self.v3_table.setItem(0, 10, sub_header_gunyuu)

        sub_header_total = QTableWidgetItem("합계")
        sub_header_total.setFont(QFont("Arial", weight=QFont.Bold))
        sub_header_total.setBackground(QBrush(QColor("#4B4B4B")))
        self.v3_table.setItem(0, 11, sub_header_total)

        # set_result_sub_header 설정
        result_sub_headers = [
            (1, "유량"), (2, "두당"), (3, "1산"),
            (4, "2산"), (5, "3산"), (6, "4산"),
            (7, "5산~"), (8, "계"), (9, "초임만삭"),
            (10, "건유우"), (11, "합계")
        ]

        for column, text in result_sub_headers:
            self.v3_sub_header(column, text)

        # 중앙 정렬 적용
        for i in range(2):  # 타이틀 행만 중앙 정렬
            for j in range(13):
                item = self.v3_table.item(i, j)
                if item:
                    item.setTextAlignment(Qt.AlignCenter)

    def v3_sub_header(self, column, text):  # v3 table sub header
        sub_result_header_item = QTableWidgetItem(text)
        sub_result_header_item.setFont(QFont("Arial", 9, QFont.Bold))
        sub_result_header_item.setBackground(QBrush(QColor("#5C5C5C")))
        self.v3_table.setItem(1, column, sub_result_header_item)

    def v3_data_table(self, data):    #v3 table data
        df = data
        # 새 데이터가 이전 데이터와 같으면 업데이트 중단
        if df is None or df.equals(self.v3_data):
            return

        # 데이터가 변경된 경우에만 테이블 업데이트 수행
        self.v3_data = df  # 새로운 데이터를 이전 데이터로 저장

        data_row_count = len(df)
        self.v3_table.setRowCount(data_row_count + 3)  # 데이터 행 + 타이틀 행 + 합계 행 포함

        # 데이터 삽입
        for row_idx, row in df.iterrows():
            for col_idx, value in enumerate(row):
                # 값이 0이면 빈 문자열로 표시
                if value == 0:
                    formatted_value = ""  # 0을 빈 문자열로 대체
                elif col_idx == 1:  # 유량 열일 경우 천 단위 구분 기호 및 소수점 두 자리까지 표시
                    formatted_value = f"{value:,.1f}"  # 천 단위 구분 기호와 소수점 두 자리 포맷
                else:
                    formatted_value = str(value)

                cell = QTableWidgetItem(formatted_value)
                cell.setTextAlignment(Qt.AlignCenter)
                self.v3_table.setItem(row_idx + 2, col_idx, cell)  # 데이터 행은 타이틀 밑에서 시작

        # 합계 행 계산 및 삽입
        total_row = data_row_count + 2  # 마지막 데이터 행 바로 아래에 합계 행 추가
        total_label_cell = QTableWidgetItem("합계")  # "합계" 텍스트 설정
        total_label_cell.setTextAlignment(Qt.AlignCenter)  # 텍스트를 가운데 정렬
        self.v3_table.setItem(total_row, 0, total_label_cell)

        # 각 열의 합계 계산
        for col_idx in range(1, self.v3_table.columnCount()):
            if col_idx in [1, 2]:  # 유량과 두당 열에 대해 합계 계산
                column_sum = df.iloc[:, col_idx].sum()  # 해당 열의 합계 계산
                if col_idx == 1:  # 유량 열일 경우 천 단위 구분 기호와 소수점 한 자리까지 표시
                    formatted_sum = f"{column_sum:,.1f}"
                else:
                    formatted_sum = str(column_sum)
            elif pd.api.types.is_numeric_dtype(df.iloc[:, col_idx]):
                column_sum = df.iloc[:, col_idx].sum()
                formatted_sum = str(column_sum) if column_sum != 0 else ""  # 숫자 열의 합계가 0이면 빈 문자열로 표시
            else:
                formatted_sum = ""  # 숫자가 아닌 열은 합계 계산 안 함

            total_cell = QTableWidgetItem(formatted_sum)
            total_cell.setTextAlignment(Qt.AlignCenter)
            self.v3_table.setItem(total_row, col_idx, total_cell)

        # 합계 행 스타일 설정
        for col_idx in range(self.v3_table.columnCount()):
            item = self.v3_table.item(total_row, col_idx)
            if item:
                item.setFont(QFont("Arial", weight=QFont.Bold))
                item.setBackground(QBrush(QColor("#763500")))  # 합계 행의 색상 설정
                item.setForeground(QBrush(QColor("white")))  # 텍스트 색상 설정

        # 테이블의 높이를 행 수에 맞게 조절
        row_height = self.v3_table.verticalHeader().defaultSectionSize()
        self.v3_table.setFixedHeight(
            row_height * self.v3_table.rowCount() + self.v3_table.horizontalHeader().height() + 2)

    """""""""""""""""""""""""""""""""""""""""""""
    start-v4 : 우군별 비육일수별 산유량 분포 (차트)
    """""""""""""""""""""""""""""""""""""""""""""
    def v4_data_chart(self, data):

        v4_chart_data = data    # 데이터가 None이면 반환

        if v4_chart_data is None:
            # 오류 발생 시 이전 데이터를 초기화하여 다음에 정상 데이터가 로드될 때 업데이트되도록 합니다.
            self.v4_data = None
            return

        # 이전 데이터와 동일한 경우 업데이트 중단 (단, self.v2_data가 None이 아닐 때만)
        if self.v4_data is not None and v4_chart_data.equals(self.v4_data):
            return

        # 데이터가 변경된 경우에만 테이블 업데이트 수행
        self.v4_data = v4_chart_data  # 새로운 데이터를 이전 데이터로 저장

        dist_fig = go.Figure()
        for birth_cnt in v4_chart_data['V_BIRTH_CNT'].unique():
            filtered_df = v4_chart_data[v4_chart_data['V_BIRTH_CNT'] == birth_cnt]
            dist_fig.add_trace(go.Scatter(
                x=filtered_df['DD'],
                y=filtered_df['milk_weight'],
                mode='markers',
                name=f"산차수 {birth_cnt}",
                marker=dict(size=8, opacity=0.7)
            ))
        dist_fig.update_layout(
            title="우군별 비유일수별 산유량 분포",
            xaxis_title="",
            yaxis_title="",
            template="plotly_dark",
            legend=dict(
                orientation="h",  # 가로 방향으로 범례 표시
                yanchor="bottom",  # y축 고정 위치를 아래쪽으로 설정
                y=1.1,  # y 위치를 위로 설정 (1보다 크게 설정)
                xanchor="center",  # x축 고정 위치를 가운데로 설정
                x=0.5,  # x 위치를 가운데로 설정
                font=dict(color="white"),  # 범례 텍스트 색상
                traceorder="normal"
            ),
        )
        # HTML 생성 및 업데이트
        dist_fig_json = dist_fig.to_json()
        dist_html_chart = self.generate_html(dist_fig_json)
        self.v4_chart_view.setHtml(dist_html_chart)

    """""""""""""""""""""""""""""""""""""""""""""
    start-v5 : 산차별 구성비 (차트)
    """""""""""""""""""""""""""""""""""""""""""""
    def v5_data_chart(self, data):
        v5_chart_data = data    # 데이터가 None이면 반환

        if v5_chart_data is None:
            # 오류 발생 시 이전 데이터를 초기화하여 다음에 정상 데이터가 로드될 때 업데이트되도록 합니다.
            self.v5_data = None
            return

        # 이전 데이터와 동일한 경우 업데이트 중단 (단, self.v2_data가 None이 아닐 때만)
        if self.v5_data is not None and v5_chart_data.equals(self.v5_data):
            return

        # 데이터가 변경된 경우에만 테이블 업데이트 수행
        self.v5_data = v5_chart_data  # 새로운 데이터를 이전 데이터로 저장

        # 열에서 각 비율 값을 직접 추출 (SP에서 이미 계산됨)
        values = [
            v5_chart_data['MILKING_1'].iloc[0],  # 위치 기반 접근을 위해 iloc 사용
            v5_chart_data['MILKING_2'].iloc[0],
            v5_chart_data['MILKING_3'].iloc[0],
            v5_chart_data['MILKING_4'].iloc[0],
            v5_chart_data['MILKING_5'].iloc[0],
        ]

        labels = ['1산', '2산', '3산', '4산', '5산+']
        colors = ['#4A90E2', '#F5A623', '#9B9B9B', '#F8E71C', '#7ED321']

        # 구성비 차트를 스택형으로 표현
        fig = go.Figure()

        # 각 항목을 별도로 추가하여 스택형으로 표시
        for i, (label, value, color) in enumerate(zip(labels, values, colors)):
            fig.add_trace(go.Bar(
                x=[value],
                y=[""],
                name=label,
                orientation='h',
                marker=dict(color=color),
                text=f"{value:.1f}%",  # 소수점 1자리로 표시
                textposition='inside',  # 텍스트가 막대 중앙에 위치하도록 설정
                textfont=dict(size=15, family="Arial Bold"),
                insidetextanchor='middle',  # 텍스트가 중앙에 정렬되도록 설정
                width=0.4  # 바의 두께를 조절
            ))

        fig.update_layout(
            barmode='stack',
            xaxis=dict(
                title="",
                tickvals=[0, 20, 40, 60, 80, 100],  # X축 범위 수동 설정
                ticktext=["0%", "20%", "40%", "60%", "80%", "100%"],  # % 기호 추가
                range=[0, 100],  # X축을 0-100%로 설정
                showgrid=True
            ),
            yaxis=dict(
                title="",
                showgrid=False
            ),
            showlegend=True,
            legend=dict(
                orientation="h",  # 가로 방향으로 범례 표시
                yanchor="bottom",  # y축 고정 위치를 아래쪽으로 설정
                y=1.1,  # y 위치를 위로 설정 (1보다 크게 설정)
                xanchor="center",  # x축 고정 위치를 가운데로 설정
                x=0.5,  # x 위치를 가운데로 설정
                font=dict(color="white"),  # 범례 텍스트 색상
                traceorder = "normal"
            ),
            title="산차별 구성비",
            template="plotly_dark",
        )

        fig_json = fig.to_json()
        html_chart = self.generate_html(fig_json)
        self.v5_chart_view.setHtml(html_chart)

    """""""""""""""""""""""""""""""""""""""""""""
    start-v6 : 두당 산유량 분포 (table)
    """""""""""""""""""""""""""""""""""""""""""""
    def v6_table_header(self):
        headers = ["산차수", "합 계", "~ 15", "16 ~ 20", "21 ~ 26", "26 ~ 30", "31 ~ 35", "36 ~ 40", "41 ~"]
        self.v6_table.setColumnCount(len(headers))
        self.v6_table.setRowCount(1)  # 첫 번째 행에 헤더 추가
        for i, header_text in enumerate(headers):
            header_item = QTableWidgetItem(header_text)
            header_item.setFont(QFont("Arial", weight=QFont.Bold))
            header_item.setTextAlignment(Qt.AlignCenter)
            header_item.setBackground(QBrush(QColor("#4B4B4B")))
            header_item.setForeground(QBrush(QColor("white")))
            self.v6_table.setItem(0, i, header_item)

    def v6_data_table(self, data):    # v6 두당 산유량 분포
        df = data
        # 새 데이터가 이전 데이터와 같으면 업데이트 중단
        if df is None or df.equals(self.v6_data):
            return

        # 데이터가 변경된 경우에만 테이블 업데이트 수행
        self.v6_data = df  # 새로운 데이터를 이전 데이터로 저장

        # 열 수를 데이터프레임의 열 수로 설정
        self.v6_table.setColumnCount(len(df.columns))
        self.v6_table.setRowCount(1 + len(df))  # 헤더 행 + 데이터 행

        # 헤더 설정 (열 이름을 테이블 상단에 추가)
        for col_idx, col_name in enumerate(df.columns):
            header_item = QTableWidgetItem(col_name)
            header_item.setFont(QFont("Arial", weight=QFont.Bold))
            header_item.setTextAlignment(Qt.AlignCenter)
            header_item.setBackground(QBrush(QColor("#4B4B4B")))
            header_item.setForeground(QBrush(QColor("white")))
            self.v6_table.setHorizontalHeaderItem(col_idx, header_item)

        # 데이터 채우기 및 합계 행 스타일 설정
        for row_idx, row in df.iterrows():
            is_total_row = (row.iloc[0] == "합계")  # 첫 번째 열이 "합계"인지 확인
            for col_idx, value in enumerate(row):
                cell = QTableWidgetItem(str(value))
                cell.setTextAlignment(Qt.AlignCenter)

                # "합계" 행의 스타일 변경
                if is_total_row:
                    cell.setBackground(QBrush(QColor("#763500")))  # 배경색 설정
                    cell.setForeground(QBrush(QColor("white")))  # 텍스트 색상 설정
                    cell.setFont(QFont("Arial", weight=QFont.Bold))  # 폰트 굵게 설정

                self.v6_table.setItem(row_idx + 1, col_idx, cell)

        # 테이블 높이를 모든 행이 보이도록 조정
        row_height = self.v6_table.verticalHeader().defaultSectionSize()
        self.v6_table.setFixedHeight((row_height * self.v6_table.rowCount()) + self.v6_table.horizontalHeader().height() + 2)

    """""""""""""""""""""""""""""""""""""""""""""
    start-v7 : 두당 산유량 구성비 (chart)
    """""""""""""""""""""""""""""""""""""""""""""
    def v7_data_chart(self, data):
        v7_chart_data = data

        if v7_chart_data is None:
            # 오류 발생 시 이전 데이터를 초기화하여 다음에 정상 데이터가 로드될 때 업데이트되도록 합니다.
            self.v7_data = None
            return

        # 이전 데이터와 동일한 경우 업데이트 중단 (단, self.v2_data가 None이 아닐 때만)
        if self.v7_data is not None and v7_chart_data.equals(self.v7_data):
            return
        # 데이터가 변경된 경우에만 테이블 업데이트 수행
        self.v7_data = v7_chart_data  # 새로운 데이터를 이전 데이터로 저장

        # CNT_1 ~ CNT_7 값을 추출합니다.
        values = [
            v7_chart_data['CNT_1'].iloc[0],
            v7_chart_data['CNT_2'].iloc[0],
            v7_chart_data['CNT_3'].iloc[0],
            v7_chart_data['CNT_4'].iloc[0],
            v7_chart_data['CNT_5'].iloc[0],
            v7_chart_data['CNT_6'].iloc[0],
            v7_chart_data['CNT_7'].iloc[0],
        ]

        labels = ['~15', '16~20', '21~26', '26~30', '31~35', '31~35', '36~40']
        colors = ['#4A90E2', '#F5A623', '#9B9B9B', '#F8E71C', '#7ED321', '#50E3C2', '#B8E986']

        # 새로운 스택형 구성비 차트를 추가
        fig = go.Figure()

        for label, value, color in zip(labels, values, colors):
            fig.add_trace(go.Bar(
                x=[value],
                y=[""],
                name=label,
                orientation='h',
                marker=dict(color=color),
                text=f"{value:.1f}%",
                textposition='inside',
                textfont=dict(size=15, family="Arial Bold"),
                insidetextanchor='middle',
                width=0.4
            ))

        fig.update_layout(
            barmode='stack',
            xaxis=dict(
                title="",
                tickvals=[0, 20, 40, 60, 80, 100],
                ticktext=["0%", "20%", "40%", "60%", "80%", "100%"],
                range=[0, 100],
                showgrid=True
            ),
            yaxis=dict(
                title="",
                showgrid=False
            ),
            showlegend=True,
            legend=dict(
                orientation="h",  # 가로 방향으로 범례 표시
                yanchor="bottom",  # y축 고정 위치를 아래쪽으로 설정
                y=1.1,  # y 위치를 위로 설정 (1보다 크게 설정)
                xanchor="center",  # x축 고정 위치를 가운데로 설정
                x=0.5,  # x 위치를 가운데로 설정
                font=dict(color="white"),  # 범례 텍스트 색상
                traceorder = "normal"
            ),
            title="두당 산유량 구성비",
            template="plotly_dark",
        )

        fig_json = fig.to_json()
        html_chart = self.generate_html(fig_json)
        self.v7_chart_view.setHtml(html_chart)

    def generate_html(self, fig_json):
        """Plotly 차트를 표시하기 위한 HTML 생성 메서드"""
        return f"""
        <html>
        <head>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        </head>
        <body style="margin:0;">
            <div id="chart" style="width:100%; height:100%;"></div>
            <script>
                var fig_data = {fig_json};
                Plotly.newPlot("chart", fig_data.data, fig_data.layout, {{responsive: true}});
            </script>
        </body>
        </html>
        """

    def create_error_overlay(self, table_widget):
        """표 위에 오류 메시지를 표시할 라벨을 생성하고 크기를 자동 조정합니다."""
        error_overlay = QLabel("데이터베이스 오류 발생", table_widget)
        error_overlay.setStyleSheet("color: red; background-color: rgba(50, 50, 50, 0.7); font-size: 15px;")
        error_overlay.setAlignment(Qt.AlignCenter)
        error_overlay.setGeometry(0, 0, table_widget.width(), table_widget.height())
        error_overlay.setWordWrap(True)  # 메시지가 길어질 경우 줄바꿈 허용
        return error_overlay

    def resizeEvent(self, event):
        """창 크기가 변경될 때마다 에러 오버레이 크기를 조정합니다."""
        super().resizeEvent(event)
        self.adjust_error_overlay_size(self.v1_error_overlay, self.v1_table)
        self.adjust_error_overlay_size(self.v3_error_overlay, self.v3_table)
        self.adjust_error_overlay_size(self.v6_error_overlay, self.v6_table)

    def adjust_error_overlay_size(self, overlay, table_widget):
        """에러 오버레이의 크기를 table_widget에 맞게 조정합니다."""
        overlay.setGeometry(0, 0, table_widget.width(), table_widget.height())

    def show_error_overlay(self, overlay, message):
        """에러 메시지 오버레이를 표시합니다."""
        overlay.setText(message)
        overlay.show()

    def hide_error_overlay(self, overlay):
        """에러 메시지 오버레이를 숨깁니다."""
        overlay.hide()

    def fetch_data_with_error_handling(self, param1, param2, target_view):
        try:
            data = fetch_data(param1, param2)  # 데이터 가져오기 시도
            if data is None or data.empty:
                raise ValueError("데이터가 없습니다.")
            # 데이터가 정상적으로 로드되면 오류 메시지 숨기기
            if isinstance(target_view, QTableWidget):
                self.hide_error_overlay(self.get_error_overlay(target_view))
            elif isinstance(target_view, QWebEngineView):
                # param1 값에 따라 각 차트 함수 호출
                if param1 == 'V2':
                    self.v2_data_chart(data)
                elif param1 == 'V4':
                    self.v4_data_chart(data)
                elif param1 == 'V5':
                    self.v5_data_chart(data)
                elif param1 == 'V7':
                    self.v7_data_chart(data)
            return data  # 성공 시 데이터 반환
        except Exception as e:
            # 오류 발생 시 메시지 표시
            error_message = f"데이터베이스 오류 발생: {str(e)}"
            if isinstance(target_view, QTableWidget):
                self.show_error_overlay(self.get_error_overlay(target_view), error_message)
            elif isinstance(target_view, QWebEngineView):
                error_html = f"<html><body style='background-color: #333; color:red; text-align:center;'><p>{error_message}</p></body></html>"
                target_view.setHtml(error_html)
            return None

    def get_error_overlay(self, table_widget):
        """주어진 QTableWidget에 대응하는 에러 오버레이를 반환합니다."""
        if table_widget == self.v1_table:
            return self.v1_error_overlay
        elif table_widget == self.v3_table:
            return self.v3_error_overlay
        elif table_widget == self.v6_table:
            return self.v6_error_overlay
        return None

# 실행
app = QApplication(sys.argv)
window = MainWindow()
window.showMaximized()
app.exec_()

import sys
import pandas as pd
from PyQt5.QtWidgets import QApplication, QTableWidget, QTableWidgetItem, QMainWindow, QVBoxLayout, QWidget, QLabel, \
    QHBoxLayout
from PyQt5.QtCore import Qt, QTimer
from PyQt5.QtGui import QColor, QBrush, QFont
from PyQt5.QtWebEngineWidgets import QWebEngineView
from PyQt5.QtWidgets import QSizePolicy, QHeaderView
from sqlalchemy import create_engine, text
import plotly.graph_objects as go

# MSSQL 데이터베이스 연결 설정
mssql_connection_string = "mssql+pyodbc://sa:ghltktjqj7%29@221.139.49.70:2433/DJNCH?driver=SQL+Server"
mssql_engine = create_engine(mssql_connection_string, fast_executemany=True)


# 데이터 가져오기 함수들
def fetch_data():
    with mssql_engine.connect() as connection:
        result = connection.execute(text("EXEC P_DASHBOARD_V1 :param1, :param2"), {'param1': 'V1', 'param2': 'M2'})
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df


def fetch_chart_data():
    with mssql_engine.connect() as connection:
        result = connection.execute(text("EXEC P_DASHBOARD_V1 :param1, :param2"), {'param1': 'V2', 'param2': 'M2'})
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        df['YMD'] = pd.to_datetime(df['YMD'], format='%Y%m%d')
    return df


def fetch_result_data():
    with mssql_engine.connect() as connection:
        result = connection.execute(text("EXEC P_DASHBOARD_V1 :param1, :param2"), {'param1': 'V3', 'param2': 'M2'})
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df

# 구성비 차트 데이터 가져오기
def fetch_composition_data():
    with mssql_engine.connect() as connection:
        result = connection.execute(text("EXEC P_DASHBOARD_V1 :param1, :param2"), {'param1': 'V5', 'param2': 'M2'})
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df

def fetch_distribution_data():
    with mssql_engine.connect() as connection:
        result = connection.execute(text("EXEC P_DASHBOARD_V1 :param1, :param2"), {'param1': 'V4', 'param2': 'M2'})
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df

def fetch_v6_m2_data():

    with mssql_engine.connect() as connection:
        result = connection.execute(text("EXEC P_DASHBOARD_V1 :param1, :param2"), {'param1': 'V6', 'param2': 'M2'})
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df

def fetch_v7_m2_data():
    with mssql_engine.connect() as connection:
        result = connection.execute(text("EXEC P_DASHBOARD_V1 :param1, :param2"), {'param1': 'V7', 'param2': 'M2'})
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Cattle Data Viewer")
        self.resize(1200, 800)

        # 메인 레이아웃 설정
        main_widget = QWidget()
        main_layout = QVBoxLayout(main_widget)

        # 좌우 테이블을 담을 레이아웃
        table_layout = QHBoxLayout()

        # 왼쪽 레이아웃
        left_layout = QVBoxLayout()

        # 좌측 상단에 우사별 사육두수 테이블 설정
        left_table_title_layout = QHBoxLayout()
        left_title_label = QLabel("  우사별 사육두수")
        left_title_label.setFont(QFont("Arial", 12, QFont.Bold))
        left_title_label.setStyleSheet("color: #763500;")
        left_table_title_layout.addWidget(left_title_label, alignment=Qt.AlignLeft)

        right_title_label = QLabel("*( )는 저지")
        right_title_label.setFont(QFont("Arial", 10, QFont.Bold))
        right_title_label.setStyleSheet("color: #00c0ff;")
        left_table_title_layout.addWidget(right_title_label, alignment=Qt.AlignRight)

        left_layout.addLayout(left_table_title_layout)

        # 사육두수 테이블 추가
        self.table = QTableWidget(0, 14, self)
        self.table.setStyleSheet("background-color: #111111; color: white; gridline-color: #5C5C5C;")
        self.table.verticalHeader().setVisible(False)
        self.table.horizontalHeader().setVisible(False)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        left_layout.addWidget(self.table)

        # 왼쪽 레이아웃의 나머지 차트 추가
        self.chart_view = QWebEngineView()
        left_layout.addWidget(self.chart_view)

        self.dist_chart_view = QWebEngineView()
        left_layout.addWidget(self.dist_chart_view)

        self.additional_dist_chart_view = QWebEngineView()
        left_layout.addWidget(self.additional_dist_chart_view)

        # 오른쪽 레이아웃
        right_layout = QVBoxLayout()

        # 사육현황 표 추가
        right_title_label = QLabel("  축주별 사육현황")
        right_title_label.setFont(QFont("Arial", 12, QFont.Bold))
        right_title_label.setStyleSheet("color: #763500;")
        right_layout.addWidget(right_title_label)

        self.result_table = QTableWidget(0, 13, self)
        self.result_table.setStyleSheet("background-color: #111111; color: white; gridline-color: #5C5C5C;")
        self.result_table.verticalHeader().setVisible(False)
        self.result_table.horizontalHeader().setVisible(False)
        self.result_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        right_layout.addWidget(self.result_table)

        # 산차별 구성비 차트 추가
        self.composition_chart_view = QWebEngineView()
        right_layout.addWidget(self.composition_chart_view)

        # 두당 산유량 분포 표 추가
        distribution_table_layout = QVBoxLayout()
        distribution_title_label = QLabel("두당 산유량 분포")
        distribution_title_label.setFont(QFont("Arial", 12, QFont.Bold))
        distribution_title_label.setStyleSheet("color: #763500;")
        distribution_table_layout.addWidget(distribution_title_label, alignment=Qt.AlignLeft)

        self.distribution_table = QTableWidget(0, 8, self)
        self.distribution_table.setStyleSheet("background-color: #111111; color: white; gridline-color: #5C5C5C;")
        self.distribution_table.verticalHeader().setVisible(False)
        self.distribution_table.horizontalHeader().setVisible(False)
        self.distribution_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.create_distribution_table_headers()
        distribution_table_layout.addWidget(self.distribution_table)
        right_layout.addLayout(distribution_table_layout)

        # 새로운 구성비 차트 추가
        self.additional_composition_chart_view = QWebEngineView()
        right_layout.addWidget(self.additional_composition_chart_view)

        # 좌우 레이아웃을 메인 레이아웃에 추가
        table_layout.addLayout(left_layout, stretch=3)
        table_layout.addLayout(right_layout, stretch=4)
        main_layout.addLayout(table_layout)

        # 메인 위젯 설정
        self.setCentralWidget(main_widget)

        # 자동 갱신 타이머 설정
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.update_tables_and_charts)
        self.timer.start(10000)  # 10초마다 새로 고침

        self.update_tables_and_charts()


    def update_tables_and_charts(self):
        self.create_group_headers()
        self.populate_table()
        self.create_result_table_headers()
        self.populate_result_table()
        self.load_chart()
        self.load_composition_chart()
        self.populate_distribution_table()
        self.load_additional_composition_chart()

    def create_distribution_table_headers(self):
        headers = ["산차수", "합 계", "~ 15", "16 ~ 20", "21 ~ 26", "26 ~ 30", "31 ~ 35", "36 ~ 40", "41 ~"]
        self.distribution_table.setColumnCount(len(headers))
        self.distribution_table.setRowCount(1)  # 첫 번째 행에 헤더 추가
        for i, header_text in enumerate(headers):
            header_item = QTableWidgetItem(header_text)
            header_item.setFont(QFont("Arial", weight=QFont.Bold))
            header_item.setTextAlignment(Qt.AlignCenter)
            header_item.setBackground(QBrush(QColor("#4B4B4B")))
            header_item.setForeground(QBrush(QColor("white")))
            self.distribution_table.setItem(0, i, header_item)


    def populate_distribution_table(self):
        # V6와 M2 파라미터로 가져온 데이터로 분포도 옆 표 채우기
        df = fetch_v6_m2_data()  # 데이터 가져오기

        # 열 수를 데이터프레임의 열 수로 설정
        self.distribution_table.setColumnCount(len(df.columns))
        self.distribution_table.setRowCount(1 + len(df))  # 헤더 행 + 데이터 행

        # 헤더 설정 (열 이름을 테이블 상단에 추가)
        for col_idx, col_name in enumerate(df.columns):
            header_item = QTableWidgetItem(col_name)
            header_item.setFont(QFont("Arial", weight=QFont.Bold))
            header_item.setTextAlignment(Qt.AlignCenter)
            header_item.setBackground(QBrush(QColor("#4B4B4B")))
            header_item.setForeground(QBrush(QColor("white")))
            self.distribution_table.setHorizontalHeaderItem(col_idx, header_item)

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

                self.distribution_table.setItem(row_idx + 1, col_idx, cell)

        # 테이블 높이를 모든 행이 보이도록 조정
        row_height = self.distribution_table.verticalHeader().defaultSectionSize()
        self.distribution_table.setFixedHeight((row_height * self.distribution_table.rowCount()) + self.distribution_table.horizontalHeader().height() + 2)

    def create_group_headers(self):
        self.table.setRowCount(2)
        # 그룹 헤더 및 하위 헤더 생성
        self.table.setSpan(0, 0, 2, 2)
        header_item = QTableWidgetItem("구분")
        header_item.setFont(QFont("Arial", weight=QFont.Bold))
        header_item.setBackground(QBrush(QColor("#4B4B4B")))
        self.table.setItem(0, 0, header_item)

        self.table.setSpan(0, 2, 2, 1)
        header_item_sum = QTableWidgetItem("합계")
        header_item_sum.setFont(QFont("Arial", weight=QFont.Bold))
        header_item_sum.setBackground(QBrush(QColor("#4B4B4B")))
        self.table.setItem(0, 2, header_item_sum)

        # 각 동 헤더 생성
        self.create_group_header("1동", 3, 4)
        self.create_group_header("2동", 5, 6)
        self.create_group_header("3동", 7, 8)
        self.create_group_header("4동", 9, 10)
        self.create_group_header("5동", 11, 12)

        # 6동 헤더 (단일 열)
        self.table.setSpan(0, 13, 2, 1)
        header_item_6dong = QTableWidgetItem("6동")
        header_item_6dong.setFont(QFont("Arial", weight=QFont.Bold))
        header_item_6dong.setBackground(QBrush(QColor("#4B4B4B")))
        self.table.setItem(0, 13, header_item_6dong)

        # 두 번째 행: 하위 헤더 (합계, 1A, 1B, ...)
        self.set_sub_header(2, "합계")
        self.set_sub_header(3, "1A")
        self.set_sub_header(4, "1B")
        self.set_sub_header(5, "2A")
        self.set_sub_header(6, "2B")
        self.set_sub_header(7, "3A")
        self.set_sub_header(8, "3B")
        self.set_sub_header(9, "4A")
        self.set_sub_header(10, "4B")
        self.set_sub_header(11, "5A")
        self.set_sub_header(12, "5B")



    def set_sub_header(self, column, text):
        sub_header_item = QTableWidgetItem(text)
        sub_header_item.setFont(QFont("Arial", 9, QFont.Bold))
        sub_header_item.setBackground(QBrush(QColor("#5C5C5C")))
        self.table.setItem(1, column, sub_header_item)

    def create_group_header(self, text, start_column, end_column):
        self.table.setSpan(0, start_column, 1, end_column - start_column + 1)
        header_item = QTableWidgetItem(text)
        header_item.setFont(QFont("Arial", weight=QFont.Bold))
        header_item.setBackground(QBrush(QColor("#4B4B4B")))
        self.table.setItem(0, start_column, header_item)

    def create_result_table_headers(self):
        """두 번째 결과 표의 헤더 생성"""
        self.result_table.setRowCount(2)  # 타이틀 행을 위해 행 개수 설정
        self.result_table.setColumnCount(12)  # 필요한 열 개수 설정

        # 첫 번째 행 - 메인 헤더 설정
        # 첫 번째 타이틀 "축주"
        self.result_table.setSpan(0, 0, 2, 1)  # 2행에 걸쳐 1열 병합
        title_item = QTableWidgetItem("축주")
        title_item.setFont(QFont("Arial", weight=QFont.Bold))
        title_item.setBackground(QBrush(QColor("#4B4B4B")))
        self.result_table.setItem(0, 0, title_item)

        # "착유량" 헤더 및 하위 항목 설정
        self.result_table.setSpan(0, 1, 1, 2)  # 3개의 열 병합
        milking_title = QTableWidgetItem("착유량")
        milking_title.setFont(QFont("Arial", weight=QFont.Bold))
        milking_title.setBackground(QBrush(QColor("#4B4B4B")))
        self.result_table.setItem(0, 1, milking_title)

        # "유량", "두당", "계" 하위 항목
        self.set_result_sub_header(1, "유량")
        self.set_result_sub_header(2, "두당")


        # "산차별 착유두수" 헤더 및 하위 항목 설정
        self.result_table.setSpan(0, 3, 1, 6)  # 6개의 열 병합
        parity_title = QTableWidgetItem("산차별 착유두수")
        parity_title.setFont(QFont("Arial", weight=QFont.Bold))
        parity_title.setBackground(QBrush(QColor("#4B4B4B")))
        self.result_table.setItem(0, 3, parity_title)

        # 하위 항목 "계", "1산", "2산", "3산", "4산", "5산~"
        self.set_result_sub_header(3, "계")
        self.set_result_sub_header(4, "1산")
        self.set_result_sub_header(5, "2산")
        self.set_result_sub_header(6, "3산")
        self.set_result_sub_header(7, "4산")
        self.set_result_sub_header(8, "5산~")


        # "총 사육두수" 헤더 및 하위 항목 설정
        self.result_table.setSpan(0, 9, 1, 3)  # 3개의 열 병합
        total_head_title = QTableWidgetItem("총 사육두수")
        total_head_title.setFont(QFont("Arial", weight=QFont.Bold))
        total_head_title.setBackground(QBrush(QColor("#4B4B4B")))
        self.result_table.setItem(0, 9, total_head_title)

        # 하위 항목 "초임만삭", "착유우", "건유우"

        self.set_result_sub_header(9, "초임만삭")
        self.set_result_sub_header(10, "착유우")
        self.set_result_sub_header(11, "건유우")

        # 중앙 정렬 적용
        for i in range(2):  # 타이틀 행만 중앙 정렬
            for j in range(13):
                item = self.result_table.item(i, j)
                if item:
                    item.setTextAlignment(Qt.AlignCenter)

    def set_result_sub_header(self, column, text):
        sub_result_header_item = QTableWidgetItem(text)
        sub_result_header_item.setFont(QFont("Arial", 9, QFont.Bold))
        sub_result_header_item.setBackground(QBrush(QColor("#5C5C5C")))
        self.result_table.setItem(1, column, sub_result_header_item)

    def populate_table(self):
        df = fetch_data()
        start_row = 2
        self.table.setRowCount(start_row + len(df))

        prev_row_index = None
        for idx, row in df.iterrows():
            row_index = start_row + idx
            if row['GROW_NM'] == "착유우":
                if prev_row_index is None:
                    prev_row_index = row_index
                    self.table.setItem(prev_row_index, 0, QTableWidgetItem(row['GROW_NM']))
                self.table.setSpan(prev_row_index, 0, row_index - prev_row_index + 1, 1)
                self.table.setItem(row_index, 1, QTableWidgetItem(row['BIRTH_CNT']))
            else:
                self.table.setSpan(row_index, 0, 1, 2)
                merged_item = QTableWidgetItem(f"{row['GROW_NM']} {row['BIRTH_CNT']}")
                self.table.setItem(row_index, 0, merged_item)

            self.table.setItem(row_index, 2, QTableWidgetItem(str(row['합계'])))
            self.table.setItem(row_index, 3, QTableWidgetItem(str(row['1A'])))
            self.table.setItem(row_index, 4, QTableWidgetItem(str(row['1B'])))
            self.table.setItem(row_index, 5, QTableWidgetItem(str(row['2A'])))
            self.table.setItem(row_index, 6, QTableWidgetItem(str(row['2B'])))
            self.table.setItem(row_index, 7, QTableWidgetItem(str(row['3A'])))
            self.table.setItem(row_index, 8, QTableWidgetItem(str(row['3B'])))
            self.table.setItem(row_index, 9, QTableWidgetItem(str(row['4A'])))
            self.table.setItem(row_index, 10, QTableWidgetItem(str(row['4B'])))
            self.table.setItem(row_index, 11, QTableWidgetItem(str(row['5A'])))
            self.table.setItem(row_index, 12, QTableWidgetItem(str(row['5B'])))
            self.table.setItem(row_index, 13, QTableWidgetItem(str(row['6A'])))

            if row['GROW_NM'] == "합계":
                for col in range(self.table.columnCount()):
                    item = self.table.item(row_index, col)
                    if item:
                        item.setBackground(QBrush(QColor("#763500")))
                        item.setForeground(QBrush(QColor("white")))
                        item.setFont(QFont("Arial", weight=QFont.Bold))

        # 중앙 정렬 설정
        for i in range(self.table.rowCount()):
            for j in range(self.table.columnCount()):
                item = self.table.item(i, j)
                if item:
                    item.setTextAlignment(Qt.AlignCenter)

        # 테이블의 높이를 행 수에 맞게 조절
        row_height = self.table.verticalHeader().defaultSectionSize()
        self.table.setFixedHeight(row_height * self.table.rowCount() + self.table.horizontalHeader().height() + 2)

    def populate_result_table(self):
        """두 번째 테이블에 데이터 추가 및 합계 행 생성"""
        df = fetch_result_data()
        data_row_count = len(df)
        self.result_table.setRowCount(data_row_count + 3)  # 데이터 행 + 타이틀 행 + 합계 행 포함

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
                self.result_table.setItem(row_idx + 2, col_idx, cell)  # 데이터 행은 타이틀 밑에서 시작

        # 합계 행 계산 및 삽입
        total_row = data_row_count + 2  # 마지막 데이터 행 바로 아래에 합계 행 추가
        total_label_cell = QTableWidgetItem("합계")  # "합계" 텍스트 설정
        total_label_cell.setTextAlignment(Qt.AlignCenter)  # 텍스트를 가운데 정렬
        self.result_table.setItem(total_row, 0, total_label_cell)

        # 각 열의 합계 계산
        for col_idx in range(1, self.result_table.columnCount()):
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
            self.result_table.setItem(total_row, col_idx, total_cell)

        # 합계 행 스타일 설정
        for col_idx in range(self.result_table.columnCount()):
            item = self.result_table.item(total_row, col_idx)
            if item:
                item.setFont(QFont("Arial", weight=QFont.Bold))
                item.setBackground(QBrush(QColor("#763500")))  # 합계 행의 색상 설정
                item.setForeground(QBrush(QColor("white")))  # 텍스트 색상 설정

        # 테이블의 높이를 행 수에 맞게 조절
        row_height = self.result_table.verticalHeader().defaultSectionSize()
        self.result_table.setFixedHeight(row_height * self.result_table.rowCount() + self.result_table.horizontalHeader().height() +2)

    def generate_html(self, fig_json):
        """Plotly 차트를 표시하기 위한 HTML 생성 메서드"""
        return f"""
        <html>
        <head>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        </head>
        <body>
            <div id="chart" style="width:100%; height:100%;"></div>
            <script>
                var fig_data = {fig_json};
                Plotly.newPlot("chart", fig_data.data, fig_data.layout, {{responsive: true}});
            </script>
        </body>
        </html>
        """

    def load_chart(self):
        # 첫 번째 차트 (일별 착유현황 추이)
        chart_data = fetch_chart_data()
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=chart_data['YMD'],
            y=chart_data['COW_CNT'],
            name='두수',
            marker_color='#007c4c',
            text=[f"{x:,}" for x in chart_data['COW_CNT']],
            textposition='inside',
            textfont=dict(color="white"),
            yaxis='y1'
        ))
        fig.add_trace(go.Scatter(
            x=chart_data['YMD'],
            y=chart_data['MILK_QTY'],
            name='착유량',
            mode='lines+markers',
            marker_color='#e59c24',
            text=[f"{x:,.2f}" for x in chart_data['MILK_QTY']],
            textposition='top center',
            textfont=dict(color="black"),
            yaxis='y2'
        ))
        fig.update_layout(
            title_text="일별 착유현황 추이 그래프(2주간)",
            template="plotly_dark",
            xaxis=dict(tickformat="%m.%d", color="white", dtick="D1"),
            yaxis=dict(title="", color="lightblue", dtick=100),
            yaxis2=dict(title="", overlaying='y', side='right', color="orange", showgrid=False, dtick=1000),
            legend=dict(orientation="h", yanchor="bottom", y=1.1, xanchor="center", x=0.5, font=dict(color="white")),
        )
        # HTML 생성 및 업데이트
        fig_json = fig.to_json()
        html_chart = self.generate_html(fig_json)
        self.chart_view.setHtml(html_chart)

        # 두 번째 차트 (분포도 차트)
        dist_data = fetch_distribution_data()
        dist_fig = go.Figure()
        for birth_cnt in dist_data['V_BIRTH_CNT'].unique():
            filtered_df = dist_data[dist_data['V_BIRTH_CNT'] == birth_cnt]
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
            legend_title="산차수"
        )
        # HTML 생성 및 업데이트
        dist_fig_json = dist_fig.to_json()
        dist_html_chart = self.generate_html(dist_fig_json)
        self.dist_chart_view.setHtml(dist_html_chart)

    def load_composition_chart(self):
        # 구성비 차트 데이터 가져오기
        data = fetch_composition_data()

        # 열에서 각 비율 값을 직접 추출 (SP에서 이미 계산됨)
        values = [
            data['MILKING_1'].iloc[0],  # 위치 기반 접근을 위해 iloc 사용
            data['MILKING_2'].iloc[0],
            data['MILKING_3'].iloc[0],
            data['MILKING_4'].iloc[0],
            data['MILKING_5'].iloc[0],
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
                width=0.6  # 바의 두께를 조절
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
            legend=dict(traceorder="normal"),  # 범례 순서 조정
            title="산차별 구성비",
            template="plotly_dark",
        )

        fig_json = fig.to_json()
        html_chart = self.generate_html(fig_json)
        self.composition_chart_view.setHtml(html_chart)


    def load_additional_composition_chart(self):
        # 새로운 구성비 차트 데이터 가져오기
        data = fetch_v7_m2_data()

        # CNT_1 ~ CNT_7 값을 추출합니다.
        values = [
            data['CNT_1'].iloc[0],
            data['CNT_2'].iloc[0],
            data['CNT_3'].iloc[0],
            data['CNT_4'].iloc[0],
            data['CNT_5'].iloc[0],
            data['CNT_6'].iloc[0],
            data['CNT_7'].iloc[0],
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
                width=0.6
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
            legend=dict(traceorder="normal"),
            title="두당 산유량 구성비",
            template="plotly_dark",
        )

        fig_json = fig.to_json()
        html_chart = self.generate_html(fig_json)
        self.additional_composition_chart_view.setHtml(html_chart)



# 실행
app = QApplication(sys.argv)
window = MainWindow()
window.showMaximized()
app.exec_()

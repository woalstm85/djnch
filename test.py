import sys
import pandas as pd
from PyQt5.QtWidgets import QApplication, QTableWidget, QTableWidgetItem, QMainWindow, QVBoxLayout, QWidget, QLabel, \
    QHBoxLayout
from PyQt5.QtCore import Qt, QTimer
from PyQt5.QtGui import QColor, QBrush, QFont
from PyQt5.QtWebEngineWidgets import QWebEngineView
from PyQt5.QtWidgets import QHeaderView
from sqlalchemy import create_engine, text
import plotly.graph_objects as go

# MSSQL 데이터베이스 연결 설정
mssql_connection_string = "mssql+pyodbc://sa:ghltktjqj7%29@221.139.49.70:2433/DJNCH?driver=SQL+Server"
mssql_engine = create_engine(mssql_connection_string, fast_executemany=True)

# 데이터 가져오기 함수들
def fetch_data(param1, param2):
    with mssql_engine.connect() as connection:
        result = connection.execute(text("EXEC P_DASHBOARD_V1 :param1, :param2"), {'param1': param1, 'param2': param2})
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
        table_layout = QHBoxLayout()

        # 좌측 레이아웃 설정
        left_layout = QVBoxLayout()
        self.create_title(left_layout, "우사별 사육두수", "#763500", "*( )는 저지", "#00c0ff")
        self.left_table = self.create_table(14)
        left_layout.addWidget(self.left_table)

        # 차트 추가
        self.chart_view = QWebEngineView()
        self.dist_chart_view = QWebEngineView()
        self.additional_dist_chart_view = QWebEngineView()
        for chart in [self.chart_view, self.dist_chart_view, self.additional_dist_chart_view]:
            left_layout.addWidget(chart)

        # 우측 레이아웃 설정
        right_layout = QVBoxLayout()
        self.create_title(right_layout, "축주별 사육현황", "#763500")
        self.result_table = self.create_table(13)
        right_layout.addWidget(self.result_table)

        # 구성비 차트 추가
        self.composition_chart_view = QWebEngineView()
        right_layout.addWidget(self.composition_chart_view)

        # 두당 산유량 분포 표 추가
        distribution_table_layout = QVBoxLayout()
        self.create_title(distribution_table_layout, "두당 산유량 분포", "#763500")
        self.distribution_table = self.create_table(8)
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
        self.setCentralWidget(main_widget)

        # 자동 갱신 타이머 설정
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.update_tables_and_charts)
        self.timer.start(10000)  # 10초마다 새로 고침

        self.update_tables_and_charts()

    def create_table(self, columns):
        """테이블 스타일을 설정하고 반환"""
        table = QTableWidget(0, columns, self)
        table.setStyleSheet("background-color: #111111; color: white; gridline-color: #5C5C5C;")
        table.verticalHeader().setVisible(False)
        table.horizontalHeader().setVisible(False)
        table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        return table

    def create_title(self, layout, text, color, extra_text=None, extra_color=None):
        """타이틀 레이블을 생성하고 추가"""
        title_layout = QHBoxLayout()
        title_label = QLabel(text)
        title_label.setFont(QFont("Arial", 12, QFont.Bold))
        title_label.setStyleSheet(f"color: {color};")
        title_layout.addWidget(title_label, alignment=Qt.AlignLeft)
        if extra_text:
            extra_label = QLabel(extra_text)
            extra_label.setFont(QFont("Arial", 10, QFont.Bold))
            extra_label.setStyleSheet(f"color: {extra_color};")
            title_layout.addWidget(extra_label, alignment=Qt.AlignRight)
        layout.addLayout(title_layout)

    def update_tables_and_charts(self):
        """테이블과 차트를 업데이트"""
        self.populate_table(fetch_data('V1', 'M2'), self.left_table)
        self.populate_table(fetch_data('V3', 'M2'), self.result_table)
        self.load_chart(fetch_data('V2', 'M2'))
        self.load_composition_chart(fetch_data('V5', 'M2'))
        self.populate_distribution_table(fetch_data('V6', 'M2'))
        self.load_additional_composition_chart(fetch_data('V7', 'M2'))

    def create_distribution_table_headers(self):
        """분포 테이블의 헤더 설정"""
        headers = ["산차수", "합 계", "~ 15", "16 ~ 20", "21 ~ 26", "26 ~ 30", "31 ~ 35", "36 ~ 40", "41 ~"]
        self.distribution_table.setColumnCount(len(headers))
        self.distribution_table.setRowCount(1)
        for i, header_text in enumerate(headers):
            header_item = QTableWidgetItem(header_text)
            header_item.setFont(QFont("Arial", weight=QFont.Bold))
            header_item.setTextAlignment(Qt.AlignCenter)
            header_item.setBackground(QBrush(QColor("#4B4B4B")))
            header_item.setForeground(QBrush(QColor("white")))
            self.distribution_table.setItem(0, i, header_item)

    def populate_table(self, df, table):
        """표에 데이터를 채우는 함수"""
        table.setRowCount(len(df) + 2)
        for idx, row in df.iterrows():
            for col_idx, value in enumerate(row):
                cell = QTableWidgetItem(str(value))
                cell.setTextAlignment(Qt.AlignCenter)
                table.setItem(idx + 2, col_idx, cell)

    def generate_html(self, fig_json):
        """Plotly 차트를 표시하기 위한 HTML 생성 메서드"""
        return f"""
        <html><head><script src="https://cdn.plot.ly/plotly-latest.min.js"></script></head>
        <body><div id="chart" style="width:100%; height:100%;"></div>
        <script>var fig_data = {fig_json};
        Plotly.newPlot("chart", fig_data.data, fig_data.layout, {{responsive: true}});</script>
        </body></html>
        """

    def load_chart(self, chart_data):
        """일별 착유현황 차트 로드"""
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=chart_data['YMD'],
            y=chart_data['COW_CNT'],
            name='두수',
            marker_color='#007c4c',
            text=[f"{x:,}" for x in chart_data['COW_CNT']],
            textposition='inside',
            textfont=dict(color="white")
        ))
        fig.update_layout(
            title_text="일별 착유현황 추이",
            template="plotly_dark"
        )
        fig_json = fig.to_json()
        self.chart_view.setHtml(self.generate_html(fig_json))

    def load_composition_chart(self, data):
        """산차별 구성비 차트 로드"""
        values = [data[f'MILKING_{i}'].iloc[0] for i in range(1, 6)]
        labels = ['1산', '2산', '3산', '4산', '5산+']
        fig = go.Figure()
        for label, value in zip(labels, values):
            fig.add_trace(go.Bar(
                x=[value],
                y=[""],
                name=label,
                orientation='h',
                text=f"{value:.1f}%",
                textposition='inside'
            ))
        fig.update_layout(barmode='stack', template="plotly_dark")
        fig_json = fig.to_json()
        self.composition_chart_view.setHtml(self.generate_html(fig_json))

    def populate_distribution_table(self, df):
        """두당 산유량 분포 표를 채우는 함수"""
        for row_idx, row in df.iterrows():
            for col_idx, value in enumerate(row):
                cell = QTableWidgetItem(str(value))
                cell.setTextAlignment(Qt.AlignCenter)
                self.distribution_table.setItem(row_idx + 1, col_idx, cell)

    def load_additional_composition_chart(self, data):
        """추가 구성비 차트 로드"""
        values = [data[f'CNT_{i}'].iloc[0] for i in range(1, 8)]
        labels = ['~15', '16~20', '21~26', '26~30', '31~35', '36~40', '41~']
        fig = go.Figure()
        for label, value in zip(labels, values):
            fig.add_trace(go.Bar(
                x=[value],
                y=[""],
                name=label,
                orientation='h',
                text=f"{value:.1f}%",
                textposition='inside'
            ))
        fig.update_layout(barmode='stack', template="plotly_dark")
        fig_json = fig.to_json()
        self.additional_composition_chart_view.setHtml(self.generate_html(fig_json))


# 실행
app = QApplication(sys.argv)
window = MainWindow()
window.showMaximized()
app.exec_()

import sys  # 시스템 관련 기능을 사용하기 위한 모듈
import pandas as pd  # 데이터 분석 및 조작을 위한 라이브러리
import plotly.graph_objects as go  # Plotly를 사용하여 대화형 차트를 생성하기 위한 모듈
from PyQt5.QtWidgets import QApplication, QMainWindow, QVBoxLayout, QWidget, QHBoxLayout, QPushButton  # PyQt5의 GUI 구성 요소
from PyQt5.QtCore import QTimer  # PyQt5에서 타이머 기능을 사용하기 위한 모듈
from PyQt5.QtWebEngineWidgets import QWebEngineView  # PyQt5에서 웹 콘텐츠를 표시하기 위한 모듈
from sqlalchemy import create_engine  # 데이터베이스 연결을 설정하기 위한 SQLAlchemy의 모듈

# MSSQL 데이터베이스 연결 문자열 설정
mssql_connection_string = "mssql+pyodbc://sa:ghltktjqj7%29@221.139.49.70:2433/DJNCH?driver=SQL+Server"
mssql_engine = create_engine(mssql_connection_string, fast_executemany=True)  # 데이터베이스 엔진 생성

class MilkWeightApp(QMainWindow):  # PyQt5의 QMainWindow 클래스를 상속받아 애플리케이션 정의
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Milk Weight Chart")  # 창 제목 설정

        # 종료 버튼 생성 및 스타일 설정
        self.exit_button = QPushButton("종료")
        self.exit_button.setStyleSheet("""
            QPushButton {
                background-color: white;
                color: black;
                border: 1px solid black;
                padding: 5px;
            }
            QPushButton:hover {
                background-color: #FFCCCC;
            }
        """)
        self.exit_button.clicked.connect(self.close_application)  # 버튼 클릭 시 애플리케이션 종료

        # 차트 자동 업데이트를 위한 타이머 설정 (10초마다 update_charts 메서드 호출)
        self.timer = QTimer()
        self.timer.timeout.connect(self.update_charts)
        self.timer.start(10000)

        # 레이아웃 구성
        self.layout = QVBoxLayout()  # 메인 레이아웃 생성
        button_layout = QHBoxLayout()  # 버튼 레이아웃 생성
        button_layout.addWidget(self.exit_button)  # 종료 버튼을 버튼 레이아웃에 추가
        self.layout.addLayout(button_layout)  # 버튼 레이아웃을 메인 레이아웃에 추가

        # 차트 레이아웃 생성
        chart_layout = QHBoxLayout()
        container = QWidget()  # 레이아웃을 감싸는 컨테이너 위젯 생성
        container.setLayout(self.layout)
        self.setCentralWidget(container)  # 컨테이너를 중앙 위젯으로 설정

        # 좌측 차트 뷰 생성 및 레이아웃에 추가
        self.left_web_view = QWebEngineView()
        chart_layout.addWidget(self.left_web_view)

        # 우측 차트 뷰 생성 및 레이아웃에 추가
        self.right_web_view = QWebEngineView()
        chart_layout.addWidget(self.right_web_view)

        # 차트 레이아웃을 메인 레이아웃에 추가
        self.layout.addLayout(chart_layout)

        # 초기 차트 로드
        self.load_initial_charts()

    def close_application(self):
        """애플리케이션을 종료하는 메서드"""
        self.close()

    def load_initial_charts(self):
        """초기 차트를 로드하는 메서드"""
        # 좌측 차트 데이터 가져오기 및 생성
        bar_df = self.fetch_bar_chart_data()
        left_fig_json = self.create_left_figure(bar_df)

        # 우측 차트 데이터 가져오기 및 생성
        line_df = self.fetch_line_chart_data()
        right_fig_json = self.create_right_figure(line_df)

        # 좌측 차트 로드
        self.left_web_view.setHtml(self.generate_html(left_fig_json))

        # 우측 차트 로드
        self.right_web_view.setHtml(self.generate_html(right_fig_json))

    def fetch_bar_chart_data(self):
        """좌측 차트 데이터를 MSSQL에서 가져오는 메서드"""
        bar_query = """
            SELECT YMD, AM_PM, SUM(milk_weight) as milk_weight
            FROM ICT_MILKING_LOG WITH(NOLOCK)
            WHERE YMD BETWEEN CONVERT(char(8), DATEADD(DAY, -7, GETDATE()), 112) AND CONVERT(char(8), GETDATE(), 112)
            GROUP BY YMD, AM_PM
            ORDER BY YMD, AM_PM
        """
        bar_df = pd.read_sql(bar_query, mssql_engine)  # 쿼리 결과를 Pandas DataFrame으로 변환
        bar_df['YMD'] = pd.to_datetime(bar_df['YMD'], format='%Y%m%d')  # 날짜 형식으로 변환
        bar_df = bar_df.sort_values(by=['YMD', 'AM_PM'])  # 정렬
        return bar_df

    def fetch_line_chart_data(self):
        """우측 차트 데이터를 MSSQL에서 가져오는 메서드"""
        line_query = """
            SELECT YMD, COUNT(DISTINCT COW_NO) as CNT, SUM(milk_weight) as milk_weight
            FROM ICT_MILKING_LOG WITH(NOLOCK)
            WHERE YMD BETWEEN CONVERT(char(8), DATEADD(DAY, -7, GETDATE()), 112) AND CONVERT(char(8), GETDATE(), 112)
            GROUP BY YMD
            ORDER BY YMD
        """
        line_df = pd.read_sql(line_query, mssql_engine)  # 쿼리 결과를 Pandas DataFrame으로 변환
        line_df['YMD'] = pd.to_datetime(line_df['YMD'], format='%Y%m%d')  # 날짜 형식으로 변환
        return line_df

    def create_left_figure(self, bar_df):
        """좌측 차트 생성 메서드"""
        fig = go.Figure()
        for am_pm, color in [('1', '#ffebcd'), ('2', '#33b27d')]:  # 오전/오후에 따라 색상 다르게 설정
            filtered_df = bar_df[bar_df['AM_PM'] == am_pm]
            fig.add_trace(
                go.Bar(
                    x=filtered_df['YMD'],
                    y=filtered_df['milk_weight'],
                    name='오전' if am_pm == '1' else '오후',
                    marker_color=color,
                    text=[f"오전: {x:,.2f}" if am_pm == '1' else f"오후: {x:,.2f}" for x in filtered_df['milk_weight']],
                    textposition='inside',
                    hovertemplate="%{x} - %{text}<extra></extra>",
                    textfont=dict(size=15)
                )
            )

        # 각 날짜에 총 착유량 표시
        for date in bar_df['YMD'].unique():
            total_milk_weight = bar_df[bar_df['YMD'] == date]['milk_weight'].sum()
            fig.add_annotation(
                x=date,
                y=total_milk_weight,
                text=f"Total: {total_milk_weight:,.2f}",
                showarrow=False,
                font=dict(size=15, color="white"),
                xanchor='center',
                yanchor='bottom'
            )

        # 차트 레이아웃 설정
        fig.update_layout(
            title=dict(text="일별 오전/오후 착유량", font=dict(color="white")),
            xaxis=dict(tickformat="%Y.%m.%d", color="white"),
            yaxis=dict(color="white"),
            legend=dict(orientation="h", yanchor="bottom", y=1.1, xanchor="center", x=0.5, font=dict(color="white")),
            font=dict(size=15),
            plot_bgcolor="#3C3F41",
            paper_bgcolor="#3C3F41",
            barmode='stack'
        )
        return fig.to_json()

    def create_right_figure(self, line_df):
        """우측 차트 생성 메서드"""
        fig = go.Figure()

        # 두수(CNT)를 막대 그래프로 추가
        fig.add_trace(go.Bar(
            x=line_df['YMD'],
            y=line_df['CNT'],
            name='두수',
            marker_color='#486297',
            text=[f"{x:,}" for x in line_df['CNT']],
            textposition='inside',
            textfont=dict(color="white"),
            yaxis='y1'
        ))

        # 착유량(milk_weight)을 꺾은선 그래프로 추가
        fig.add_trace(go.Scatter(
            x=line_df['YMD'],
            y=line_df['milk_weight'],
            name='착유량',
            mode='lines+markers',
            marker_color='#e59c24',
            text=[f"{x:,.2f}" for x in line_df['milk_weight']],
            textposition='top center',
            textfont=dict(color="black"),
            yaxis='y2'
        ))

        # 차트 레이아웃 설정
        fig.update_layout(
            title=dict(text="일별 두수 및 착유량", font=dict(color="white")),
            xaxis=dict(
                tickformat="%Y.%m.%d",
                color="white"
            ),
            yaxis=dict(
                title="",
                color="lightblue",
                dtick=100
            ),
            yaxis2=dict(
                title="",
                overlaying='y',
                side='right',
                color="orange",
                showgrid=False,
                dtick=500
            ),
            legend=dict(
                orientation="h",
                yanchor="bottom", y=1.1,
                xanchor="center", x=0.5,
                font=dict(color="white")
            ),
            font=dict(size=15),
            plot_bgcolor="#3C3F41",
            paper_bgcolor="#3C3F41"
        )
        return fig.to_json()

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

    def update_charts(self):
        """차트를 10초마다 업데이트하는 메서드"""
        # 업데이트된 데이터를 가져와 좌측 차트 업데이트
        new_bar_df = self.fetch_bar_chart_data()
        new_left_fig_json = self.create_left_figure(new_bar_df)
        self.left_web_view.setHtml(self.generate_html(new_left_fig_json))

        # 업데이트된 데이터를 가져와 우측 차트 업데이트
        new_line_df = self.fetch_line_chart_data()
        new_right_fig_json = self.create_right_figure(new_line_df)
        self.right_web_view.setHtml(self.generate_html(new_right_fig_json))

if __name__ == "__main__":
    app = QApplication(sys.argv)  # PyQt5 애플리케이션 객체 생성
    main_app = MilkWeightApp()  # MilkWeightApp 인스턴스 생성
    main_app.showFullScreen()  # 전체 화면으로 애플리케이션 표시
    sys.exit(app.exec_())  # 애플리케이션 이벤트 루프 시작

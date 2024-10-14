import sys
import pandas as pd
import matplotlib.pyplot as plt
from PyQt5.QtWidgets import QApplication, QMainWindow, QVBoxLayout, QWidget
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from PyQt5.QtCore import QTimer
from sqlalchemy import create_engine

# MSSQL connection string
mssql_connection_string = "mssql+pyodbc://sa:ghltktjqj7%29@221.139.49.70:2433/DJNCH?driver=SQL+Server"
mssql_engine = create_engine(mssql_connection_string, fast_executemany=True)


# 데이터베이스에서 데이터를 가져오는 함수
def get_data():
    query = """
        SELECT YMD, COW_NO, SUM(milk_weight) as milk_weight
        FROM ICT_MILKING_LOG WITH(NOLOCK)
        WHERE YMD = CONVERT(char(8), GETDATE(), 112)
        GROUP BY YMD, COW_NO
        ORDER BY YMD, COW_NO
    """
    df = pd.read_sql(query, mssql_engine)
    return df


# 막대 차트를 그리는 함수
def draw_chart(df, ax):
    df['YMD'] = pd.to_datetime(df['YMD'], format='%Y%m%d').dt.strftime('%Y.%m.%d')
    latest_date = df['YMD'].unique()[0]

    # 차트 그리기
    ax.clear()
    ax.bar(df['COW_NO'], df['milk_weight'], color=plt.colormaps.get_cmap('tab20').colors)  # 막대 차트
    ax.set_title(f'Milk Weight by Cow ({latest_date})', fontsize=14)
    ax.set_xlabel('Cow Number')
    ax.set_ylabel('Weight')
    ax.tick_params(axis='x', rotation=45)

    # 값 표시
    for idx, value in enumerate(df['milk_weight']):
        ax.text(idx, value, f'{value:.2f}', ha='center', va='bottom')


# PyQt5 창 생성
class App(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle('Milk Weight by Cow')
        self.setGeometry(100, 100, 800, 600)

        # 레이아웃 설정
        self.widget = QWidget()
        self.setCentralWidget(self.widget)
        self.layout = QVBoxLayout(self.widget)

        # matplotlib FigureCanvas 생성
        self.figure, self.ax = plt.subplots()
        self.canvas = FigureCanvas(self.figure)
        self.layout.addWidget(self.canvas)

        # 데이터 관련 변수
        self.df = get_data()
        self.current_start_index = 0

        # 타이머 설정 (10초마다 차트 갱신)
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.update_chart)
        self.timer.start(10000)  # 10000 밀리초 = 10초

        # 첫 차트 그리기
        self.update_chart()

    def update_chart(self):
        # 데이터를 30개씩 슬라이싱
        start_index = self.current_start_index
        end_index = start_index + 30
        sliced_df = self.df.iloc[start_index:end_index]

        # 차트 업데이트
        draw_chart(sliced_df, self.ax)
        self.canvas.draw()

        # 인덱스 갱신 (순환)
        self.current_start_index = (self.current_start_index + 30) % len(self.df)


if __name__ == '__main__':
    app = QApplication(sys.argv)
    main_window = App()
    main_window.show()
    sys.exit(app.exec_())

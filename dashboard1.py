import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

# MSSQL connection string
mssql_connection_string = "mssql+pyodbc://sa:ghltktjqj7%29@221.139.49.70:2433/DJNCH?driver=SQL+Server"
mssql_engine = create_engine(mssql_connection_string, fast_executemany=True)

# Dash 앱 생성
app = dash.Dash(__name__)

# 인터벌 설정 (10초마다 데이터 갱신)
app.layout = html.Div(children=[
    dcc.Interval(
        id='interval-component',
        interval=10 * 1000,  # 10초마다 데이터 갱신 (밀리초 단위)
        n_intervals=0
    ),

    # 막대 차트 영역 (세로 높이 조절 가능하게 설정, 중앙 정렬)
    html.Div(children=[
        dcc.Graph(
            id='cow-weight-bar-chart',
            style={'height': '90vh', 'width': '100%'}  # 세로 높이와 가로 너비 설정
        )
    ], style={'display': 'flex', 'justify-content': 'center', 'align-items': 'center'})  # 중앙 정렬 스타일 추가
])

# 데이터를 가져와 막대 차트를 업데이트하는 콜백 함수
@app.callback(
    Output('cow-weight-bar-chart', 'figure'),
    [Input('interval-component', 'n_intervals')]
)
def update_bar_chart(n):
    # SQL 쿼리 (YMD, COW_NO 별 우유 무게)
    query = """
        SELECT YMD, COW_NO, SUM(milk_weight) as milk_weight
        FROM ICT_MILKING_LOG WITH(NOLOCK)
        WHERE YMD = CONVERT(char(8), GETDATE(), 112)
        GROUP BY YMD, COW_NO
        ORDER BY YMD, COW_NO
    """

    # MSSQL에서 데이터를 가져옴
    df = pd.read_sql(query, mssql_engine)

    # 30개씩 데이터를 슬라이싱
    start_index = (n * 30) % len(df)
    end_index = start_index + 30
    sliced_df = df.iloc[start_index:end_index].copy()

    # YMD 컬럼을 날짜 형식으로 변환
    sliced_df['YMD'] = pd.to_datetime(sliced_df['YMD'], format='%Y%m%d').dt.strftime('%Y.%m.%d')

    # 가장 최신 날짜 선택
    latest_date = sliced_df['YMD'].unique()[0]

    # 막대 차트 생성 (막대 안에 값 표시, 각 막대의 색상 다르게 설정)
    bar_fig = px.bar(
        sliced_df,
        x='COW_NO',
        y='milk_weight',
        text='milk_weight',  # 막대 안에 값 표시
        title=f"Milk Weight by Cow ({latest_date})",  # 제목에 날짜 추가
        labels={'milk_weight': 'Weight', 'COW_NO': 'Cow Number'},
        color='COW_NO',  # 막대 색상을 COW_NO 값으로 다르게 설정
        color_discrete_sequence=px.colors.qualitative.Plotly  # 각기 다른 색상 설정
    )

    # 막대 안에 값의 위치 조정
    bar_fig.update_traces(
        texttemplate='%{text:.2f}',  # 소수점 두 자리까지 표시
        textposition='outside'  # 값을 막대 안에 표시
    )

    # 막대 차트의 레이아웃 조정 (범례 제거 및 제목 정렬)
    bar_fig.update_layout(
        title=dict(
            text=f"<b>Milk Weight by Cow ({latest_date})</b>",  # 제목에 날짜 추가
            x=0.5,  # 제목을 중앙 정렬
            xanchor='center',
            yanchor='top'
        ),
        xaxis_title="",
        yaxis_title="",
        margin=dict(l=40, r=40, t=40, b=40),  # 차트 여백 조정
        showlegend=False  # 범례 제거
    )

    return bar_fig

# Dash 앱 실행
if __name__ == '__main__':
    app.run_server(debug=True)

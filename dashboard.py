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

# 인터벌 설정 (5초마다 데이터 갱신)
app.layout = html.Div(children=[
    dcc.Interval(
        id='interval-component',
        interval=10 * 1000,  # 5초마다 데이터 갱신 (밀리초 단위)
        n_intervals=0
    ),

    # 막대 차트 영역
    html.Div(children=[
        dcc.Graph(
            id='milk-weight-bar-chart',
            style={'height': '45vh', 'width': '100%'}  # 높이는 50% 뷰포트, 가로는 100%
        )
    ], style={'width': '100%', 'display': 'inline-block'}),  # 차트를 가로 100%로 설정

    # 선 차트 영역
    html.Div(children=[
        dcc.Graph(
            id='milk-weight-line-chart',
            style={'height': '45vh', 'width': '100%'}  # 높이는 50% 뷰포트, 가로는 100%
        )
    ], style={'width': '100%', 'display': 'inline-block'})  # 차트를 가로 100%로 설정
])


# 데이터를 가져와 막대 차트와 선 차트를 업데이트하는 콜백 함수
@app.callback(
    [Output('milk-weight-bar-chart', 'figure'),
     Output('milk-weight-line-chart', 'figure')],
    [Input('interval-component', 'n_intervals')]
)
def update_charts(n):
    # 막대 차트를 위한 SQL 쿼리 (YMD, AM_PM 별 우유 무게)
    bar_query = """
        SELECT YMD, AM_PM, SUM(milk_weight) as milk_weight
        FROM ICT_MILKING_LOG WITH(NOLOCK)
        WHERE YMD BETWEEN CONVERT(char(8), DATEADD(DAY, -7, GETDATE()), 112) AND CONVERT(char(8), GETDATE(), 112)
        GROUP BY YMD, AM_PM
        ORDER BY YMD, AM_PM
    """

    # 선 차트를 위한 SQL 쿼리 (날짜별 우유 무게)
    line_query = """
        SELECT YMD, SUM(milk_weight) as milk_weight
        FROM ICT_MILKING_LOG WITH(NOLOCK)
        WHERE YMD BETWEEN CONVERT(char(8), DATEADD(DAY, -7, GETDATE()), 112) AND CONVERT(char(8), GETDATE(), 112)
        GROUP BY YMD
        ORDER BY YMD
    """

    # MSSQL에서 데이터를 가져옴
    bar_df = pd.read_sql(bar_query, mssql_engine)
    bar_df['YMD'] = pd.to_datetime(bar_df['YMD'], format='%Y%m%d')

    line_df = pd.read_sql(line_query, mssql_engine)
    line_df['YMD'] = pd.to_datetime(line_df['YMD'], format='%Y%m%d')

    # 데이터가 제대로 정렬되도록 YMD와 AM_PM 기준으로 정렬
    bar_df = bar_df.sort_values(by=['YMD', 'AM_PM'])

    # Add "오전" or "오후" label to milk_weight for displaying inside bars
    bar_df['text_label'] = bar_df.apply(
        lambda row: f"오전: {row['milk_weight']:.2f}" if row['AM_PM'] == '1' else f"오후: {row['milk_weight']:.2f}",
        axis=1
    )

    # 막대 차트 생성 (YMD, AM_PM 별 우유 무게)
    bar_fig = px.bar(bar_df, x='YMD', y='milk_weight', color='AM_PM', text='text_label',
                     labels={'milk_weight': '', 'AM_PM': ''},  # 범례 제목 제거
                     title="", barmode='stack',
                     color_discrete_map={'1': '#3DC2BD', '2': '#F482A3'})  # 오전(1): 청록색, 오후(2): 핑크색

    # Update legend names to "오전" and "오후"
    bar_fig.for_each_trace(lambda t: t.update(name={'1': '오전', '2': '오후'}[t.name]))

    # Adjust hovertemplate to show date and milk weight in desired format
    bar_fig.update_traces(
        texttemplate='%{text}',  # Display text inside bars
        textposition='inside',  # Position text inside bars
        hovertemplate="<b>일자: %{x}</b><br>" +  # Show date (YMD)
                      "%{text}<extra></extra>"  # Show '오전: 값' or '오후: 값'
    )

    # 각 YMD별 합계를 계산하여 표시
    for date in bar_df['YMD'].unique():
        total_milk_weight = bar_df[bar_df['YMD'] == date]['milk_weight'].sum()

        # 막대 위에 합계 추가
        bar_fig.add_annotation(
            x=date,
            y=total_milk_weight,
            text=f"Total: {total_milk_weight:.2f}",
            showarrow=False,
            font=dict(size=12, color="black"),
            xanchor='center',
            yanchor='bottom'
        )

    # 범례 없애기 및 제목과 간격 조정
    bar_fig.update_layout(
        showlegend=False,  # 범례 제거
        title=dict(
            text="<b>Daily Milk Weight by AM/PM</b>",  # 제목 추가
            x=0.04,  # 제목을 왼쪽으로 정렬 (0.05은 약간 왼쪽 정렬)
            xanchor='left',
            yanchor='top',
            pad=dict(b=15),  # 제목과 차트 간의 간격을 줄임
            font=dict(size=20),
        ),
        xaxis_title="",
        yaxis_title="",
        xaxis=dict(
            tickformat='%Y.%m.%d'  # 날짜 포맷 (YYYY.MM.DD)
        ),
        margin=dict(l=40, r=40, t=40, b=40)  # 차트 여백 조정
    )

    # 선 차트 생성 (날짜별 우유 무게)
    line_fig = px.line(line_df, x='YMD', y='milk_weight', labels={'milk_weight': 'Total Milk Weight', 'YMD': 'Date'},
                       title="", markers=True)

    # 선 차트에서 각 점에 값 표시 및 선 색상 변경
    line_fig.update_traces(
        texttemplate='%{y:.2f}',  # 각 점에 우유 무게 표시
        textposition='bottom center',  # 텍스트를 점 아래에 중앙 정렬로 표시
        line=dict(color='red'),  # 선 색상을 붉은색으로 변경
        marker=dict(size=8, color='red'),  # 마커 크기 증가 및 색상 맞춤
        mode='markers+lines+text'
    )

    # 최소값과 최대값에 수평선 추가
    min_value = line_df['milk_weight'].min()
    max_value = line_df['milk_weight'].max()

    line_fig.add_hline(y=min_value, line_dash="dash", line_color="blue", annotation_text="",
                       annotation_position="top left")
    line_fig.add_hline(y=max_value, line_dash="dash", line_color="green", annotation_text="",
                       annotation_position="top left")

    # 선 차트에서 각 점에 값 표시 및 스타일 적용
    line_fig.update_traces(
        texttemplate=[
            f"<b style='color:blue;'>%{{y:.2f}}</b>" if y == min_value else
            f"<b style='color:red;'>%{{y:.2f}}</b>" if y == max_value else
            "%{y:.2f}" for y in line_df['milk_weight']
        ],
        textposition='bottom center',  # 텍스트를 점 아래 중앙에 표시
        line=dict(color='red'),  # 선 색상을 붉은색으로 변경
        marker=dict(size=8, color='red'),  # 마커 크기 증가 및 색상 맞춤
        mode='markers+lines+text'  # 마커, 선, 텍스트 모두 표시
    )

    # 선 차트의 날짜 포맷 변경
    line_fig.update_layout(
        title=dict(
            text="<b>Daily Milk Weight</b>",  # 제목 추가
            x=0.04,  # 제목을 왼쪽으로 정렬
            xanchor='left',
            yanchor='top',
            pad=dict(b=15),  # 제목과 차트 간의 간격을 줄임
            font=dict(size=20),
        ),
        xaxis_title="",
        yaxis_title="",
        xaxis=dict(
            tickformat='%Y.%m.%d'  # 날짜 포맷 (YYYY.MM.DD)
        ),
        margin=dict(l=40, r=40, t=40, b=40)  # 차트 여백 조정
    )

    return bar_fig, line_fig


# Dash 앱 실행
if __name__ == '__main__':
    app.run_server(debug=True)

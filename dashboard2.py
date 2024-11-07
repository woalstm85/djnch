import plotly.graph_objects as go

# 각 항목의 구성비 비율 예시 (단위: 퍼센트)
categories = ['산차 1', '산차 2', '산차 3', '산차 4', '산차 5+']
values = [30, 20, 15, 10, 25]  # 각 산차별 비율 예시

# 스택형 막대 차트 생성
fig = go.Figure(go.Bar(
    x=values,
    y=['산차별 구성비'],
    orientation='h',
    text=[f"{val}%" for val in values],
    textposition='inside',
    marker=dict(
        color=['#4A90E2', '#F5A623', '#9B9B9B', '#F8E71C', '#7ED321']
    ),
    width=0.2  # 막대 높이 조정
))

# 레이아웃 설정
fig.update_layout(
    barmode='stack',
    xaxis=dict(
        tickformat="%",
        range=[0, 100],
        showgrid=True
    ),
    showlegend=False,
    title="산차별 구성비"
)

fig.show()

from sqlalchemy import create_engine
from shapely.geometry import Point, Polygon
import pandas as pd


def find_area(current_lat, current_lng):
    # DB 연결
    engine = create_engine(
        "mssql+pyodbc://sa:ghltktjqj7%29@221.139.49.70:2433/DJNCH"
        "?driver=SQL+Server"
        "&TrustServerCertificate=yes"
    )

    # DB에서 좌표 데이터 가져오기
    query = "SELECT * FROM areas"
    areas_df = pd.read_sql(query, engine, index_col=None)

    # 현재 위치 포인트 생성
    current_point = Point(current_lat, current_lng)

    # 각 영역 확인
    for _, area in areas_df.iterrows():
        # 영역의 꼭지점으로 폴리곤 생성
        polygon = Polygon([
            (float(area['point1_lat']), float(area['point1_lng'])),
            (float(area['point2_lat']), float(area['point2_lng'])),
            (float(area['point4_lat']), float(area['point4_lng'])),
            (float(area['point3_lat']), float(area['point3_lng']))
        ])

        # 현재 위치가 이 영역 안에 있는지 확인
        if polygon.contains(current_point):
            return area['name']  # 포함된 영역을 찾으면 해당 이름 반환

    return None  # 어떤 영역에도 포함되지 않으면 None 반환


def main():
    # 현재 좌표
    current_lat = 37.503780
    current_lng = 126.790425

    # 위치가 포함된 영역 찾기
    area_name = find_area(current_lat, current_lng)

    # 결과 출력
    if area_name:
        print(f"현재 위치는 '{area_name}' 영역에 포함됩니다.")
    else:
        print("현재 위치는 어떤 영역에도 포함되지 않습니다.")


if __name__ == "__main__":
    main()
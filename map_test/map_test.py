from sqlalchemy import create_engine
import pandas as pd


def get_coordinates_from_db():
    engine = create_engine(
        "mssql+pyodbc://sa:ghltktjqj7%29@221.139.49.70:2433/DJNCH"
        "?driver=SQL+Server"
        "&TrustServerCertificate=yes"
    )

    query = "SELECT * FROM areas"
    areas_df = pd.read_sql(query, engine)
    return areas_df


def create_map_html(areas_df):
    html_content = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Area Map</title>
    <style>
        html, body { height: 100%; margin: 0; padding: 0; }
        #map { height: 100vh; width: 100%; }
    </style>
</head>
<body>
    <div id="map"></div>
    <script>
        function initMap() {
            const map = new google.maps.Map(document.getElementById("map"), {
                zoom: 17,
                center: { lat: 37.503994, lng: 126.789820 }
            });

            // 현재 위치 마커
            const currentPosition = { lat: 37.503780, lng: 126.790425 };
            const currentMarker = new google.maps.Marker({
                position: currentPosition,
                map: map,
                title: "현재 위치",
                icon: {
                    path: google.maps.SymbolPath.CIRCLE,
                    fillColor: "#FFFFFF",
                    fillOpacity: 1,
                    strokeColor: "#000000",
                    strokeWeight: 1,
                    scale: 10
                }
            });
    """

    colors = ["#FF0000", "#0000FF", "#00FF00"]

    for idx, area in areas_df.iterrows():
        color = colors[idx % len(colors)]

        # 삼각형인 경우
        if pd.isna(area['point4_lat']) or pd.isna(area['point4_lng']):
            coords = f"""
                const area{idx}Coords = [
                    {{ lat: {area['point1_lat']}, lng: {area['point1_lng']} }},  // 1번 점
                    {{ lat: {area['point2_lat']}, lng: {area['point2_lng']} }},  // 2번 점
                    {{ lat: {area['point3_lat']}, lng: {area['point3_lng']} }},  // 3번 점
                    {{ lat: {area['point1_lat']}, lng: {area['point1_lng']} }}   // 다시 1번 점으로
                ];
            """
        else:  # 사각형인 경우: 1->2->4->3->1 순서로 연결
            coords = f"""
                const area{idx}Coords = [
                    {{ lat: {area['point1_lat']}, lng: {area['point1_lng']} }},  // 1번 점
                    {{ lat: {area['point2_lat']}, lng: {area['point2_lng']} }},  // 2번 점
                    {{ lat: {area['point4_lat']}, lng: {area['point4_lng']} }},  // 4번 점
                    {{ lat: {area['point3_lat']}, lng: {area['point3_lng']} }},  // 3번 점
                    {{ lat: {area['point1_lat']}, lng: {area['point1_lng']} }}   // 다시 1번 점으로
                ];
            """

        polygon = f"""
            {coords}
            const area{idx} = new google.maps.Polygon({{
                paths: area{idx}Coords,
                strokeColor: "{color}",
                strokeWeight: 2,
                fillColor: "{color}",
                fillOpacity: 0.35
            }});
            area{idx}.setMap(map);

            // 영역에 마우스를 올렸을 때 정보창 표시
            const infoWindow{idx} = new google.maps.InfoWindow({{
                content: "{area['name']}"
            }});

            area{idx}.addListener('mouseover', function(e) {{
                infoWindow{idx}.setPosition(e.latLng);
                infoWindow{idx}.open(map);
            }});

            area{idx}.addListener('mouseout', function() {{
                infoWindow{idx}.close();
            }});
        """
        html_content += polygon

    html_content += """
        }
    </script>
    <script src="https://maps.googleapis.com/maps/api/js?key=AIzaSyDNiiHzhqOX79XJjQ6gHyFd9dGIfyekJJw&callback=initMap" async defer></script>
</body>
</html>
    """

    with open('map.html', 'w', encoding='utf-8') as f:
        f.write(html_content)


def main():
    areas_df = get_coordinates_from_db()
    create_map_html(areas_df)
    print("map.html 파일이 생성되었습니다.")


if __name__ == "__main__":
    main()
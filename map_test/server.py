import http.server
import socketserver
import socket
import webbrowser
import os


def get_ip():
    # 현재 컴퓨터의 IP 주소를 가져옴
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)
    return ip_address


# 현재 디렉토리로 작업 디렉토리 변경
os.chdir(os.path.dirname(os.path.abspath(__file__)))

PORT = 8000
ip_address = get_ip()

Handler = http.server.SimpleHTTPRequestHandler

# 0.0.0.0으로 설정하여 외부 접속 허용
with socketserver.TCPServer(("0.0.0.0", PORT), Handler) as httpd:
    print(f"서버가 시작되었습니다.")
    print(f"같은 네트워크의 다른 PC에서 접속하려면: http://{ip_address}:{PORT}/map.html")
    print(f"현재 PC에서 접속하려면: http://localhost:{PORT}/map.html")

    # 브라우저에서 페이지 열기
    webbrowser.open(f'http://localhost:{PORT}/map.html')

    # 서버 실행
    httpd.serve_forever()
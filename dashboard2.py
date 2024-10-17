import tkinter as tk
import cv2
from PIL import Image, ImageTk
import threading
import requests
import numpy as np
import random


# CCTV 데이터를 가져오는 함수
def get_cctv_url(lat, lng):
    minX = str(lng - 1)
    maxX = str(lng + 1)
    minY = str(lat - 1)
    maxY = str(lat + 1)

    api_call = 'https://openapi.its.go.kr:9443/cctvInfo?' \
               'apiKey=4d4c2f232c914418b8a370f0f08eb584' \
               '&type=ex&cctvType=2' \
               '&minX=' + minX + \
               '&maxX=' + maxX + \
               '&minY=' + minY + \
               '&maxY=' + maxY + \
               '&getType=json'

    try:
        w_dataset = requests.get(api_call).json()
        cctv_data = w_dataset['response']['data']
    except (KeyError, requests.RequestException) as e:
        print("Error fetching CCTV data:", e)
        return None

    coordx_list = []
    for index, data in enumerate(cctv_data):
        xy_couple = (float(cctv_data[index]['coordy']), float(cctv_data[index]['coordx']))
        coordx_list.append(xy_couple)

    coordx_list = np.array(coordx_list)
    leftbottom = np.array((lat, lng))
    distances = np.linalg.norm(coordx_list - leftbottom, axis=1)
    min_index = np.argmin(distances)

    return cctv_data[min_index] if cctv_data else None


# 임의의 좌표 리스트 (6개)
coordinates = [
    (36.58629, 128.186793),
    (37.5665, 126.9780),  # 서울
    (35.1796, 129.0756),  # 부산
    (37.4563, 126.7052),  # 인천
    (35.8722, 128.6025),  # 대구
    (35.1595, 126.8526),  # 광주
]


# 두 개의 CCTV를 번갈아 가며 표시하는 함수
def switch_cctv(cctv_labels, cctv_data_list, root):
    def update():
        # 두 개의 CCTV 선택
        cctv_data1 = random.choice(cctv_data_list)
        cctv_data2 = random.choice(cctv_data_list)

        # CCTV 스트림 URL을 사용하여 화면 표시
        update_cctv_frame(cctv_labels[0], cctv_data1['cctvurl'])
        update_cctv_frame(cctv_labels[1], cctv_data2['cctvurl'])

        root.after(10000, update)  # 10초마다 호출

    update()


# CCTV 화면을 업데이트하는 함수
def update_cctv_frame(frame_label, cctv_url):
    cap = cv2.VideoCapture(cctv_url)  # CCTV 스트림 열기
    if not cap.isOpened():
        print(f"Cannot open stream: {cctv_url}")
        return

    def stream():
        ret, frame = cap.read()
        if ret:
            frame = cv2.resize(frame, (320, 240))
            img = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            img_pil = Image.fromarray(img)
            imgtk = ImageTk.PhotoImage(image=img_pil)
            frame_label.imgtk = imgtk
            frame_label.configure(image=imgtk)  # Label에 이미지를 업데이트
        frame_label.after(50, stream)  # 50ms마다 프레임 갱신 (20fps 정도)

    # 50ms마다 새로운 프레임을 읽어오기
    frame_label.after(50, stream)


# Tkinter로 GUI 생성
def create_gui(cctv_data_list):
    root = tk.Tk()
    root.title("CCTV Viewer")

    # CCTV 영역
    cctv_frame = tk.Frame(root)
    cctv_frame.pack(side=tk.LEFT)

    cctv_labels = []

    # CCTV 화면 2개
    for i in range(2):
        cctv_label = tk.Label(cctv_frame)
        cctv_label.pack(pady=10)
        cctv_labels.append(cctv_label)

    # 10초마다 CCTV 화면 변경
    switch_cctv(cctv_labels, cctv_data_list, root)

    root.mainloop()


# CCTV 데이터를 가져와서 리스트에 저장
cctv_data_list = []
for lat, lng in coordinates:
    cctv_data = get_cctv_url(lat, lng)
    if cctv_data:
        cctv_data_list.append(cctv_data)

# GUI 생성 및 시작
if __name__ == "__main__":
    if cctv_data_list:
        create_gui(cctv_data_list)
    else:
        print("No CCTV data available.")

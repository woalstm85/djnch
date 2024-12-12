import sys
import cv2
import numpy as np
import pytesseract
from PIL import Image
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtGui import *

# Tesseract 경로 지정
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'


class DigitalClockOCR(QMainWindow):
    def __init__(self):
        super().__init__()
        self.initUI()
        self.initCamera()

    def initUI(self):
        # 메인 위젯과 레이아웃 설정
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)

        # 이미지 표시 영역 레이아웃
        image_layout = QHBoxLayout()

        # 원본 이미지 레이블
        self.camera_label = QLabel('카메라')
        self.camera_label.setFixedSize(320, 240)
        image_layout.addWidget(self.camera_label)

        # 처리된 이미지들을 표시할 레이블들
        self.processed_labels = {}
        for name in ['Binary', 'Mask', 'Red Only']:
            label = QLabel(name)
            label.setFixedSize(320, 240)
            self.processed_labels[name.lower()] = label
            image_layout.addWidget(label)

        layout.addLayout(image_layout)

        # 결과 표시 영역
        result_layout = QHBoxLayout()

        # 캡처 버튼
        self.capture_btn = QPushButton('캡처', self)
        self.capture_btn.clicked.connect(self.capture_image)
        result_layout.addWidget(self.capture_btn)

        # 결과 텍스트 표시
        self.result_label = QLabel('인식 결과: ')
        result_layout.addWidget(self.result_label)

        layout.addLayout(result_layout)

        # 윈도우 설정
        self.setWindowTitle('디지털 시계 OCR')
        self.setGeometry(100, 100, 1300, 400)

        # 타이머 설정 (카메라 프레임 업데이트용)
        self.timer = QTimer()
        self.timer.timeout.connect(self.update_frame)
        self.timer.start(30)  # 30ms 간격으로 업데이트

    def initCamera(self):
        self.cap = cv2.VideoCapture(0)
        if not self.cap.isOpened():
            QMessageBox.critical(self, "에러", "카메라를 열 수 없습니다.")
            sys.exit()

    def update_frame(self):
        ret, frame = self.cap.read()
        if ret:
            # 카메라 프레임 표시
            frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            h, w, ch = frame_rgb.shape
            bytes_per_line = ch * w
            qt_image = QImage(frame_rgb.data, w, h, bytes_per_line, QImage.Format_RGB888)
            scaled_image = qt_image.scaled(320, 240, Qt.KeepAspectRatio)
            self.camera_label.setPixmap(QPixmap.fromImage(scaled_image))

    def capture_image(self):
        ret, frame = self.cap.read()
        if ret:
            # 이미지 저장
            cv2.imwrite('captured.jpg', frame)

            # OCR 처리
            result = self.recognize_digital_numbers('captured.jpg')
            self.result_label.setText(f'인식 결과: {result if result else "인식 실패"}')

            # 처리된 이미지들 표시
            self.display_processed_images()

    def display_processed_images(self):
        # 저장된 이미지들 불러와서 표시
        image_files = {
            'binary': 'binary.jpg',
            'mask': 'mask.jpg',
            'red only': 'red_only.jpg'
        }

        for name, file in image_files.items():
            try:
                img = cv2.imread(file)
                if img is not None:
                    img_rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
                    h, w, ch = img_rgb.shape
                    bytes_per_line = ch * w
                    qt_image = QImage(img_rgb.data, w, h, bytes_per_line, QImage.Format_RGB888)
                    scaled_image = qt_image.scaled(320, 240, Qt.KeepAspectRatio)
                    self.processed_labels[name].setPixmap(QPixmap.fromImage(scaled_image))
            except Exception as e:
                print(f"이미지 표시 에러 ({file}): {str(e)}")

    def recognize_digital_numbers(self, image_path):
        """
        디지털 숫자를 인식하는 함수
        """
        try:
            # 이미지 읽기
            image = cv2.imread(image_path)

            # HSV 변환
            hsv = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)

            # 진한 빨간색 범위 설정
            lower_red1 = np.array([0, 120, 120])
            upper_red1 = np.array([10, 255, 255])
            mask1 = cv2.inRange(hsv, lower_red1, upper_red1)

            lower_red2 = np.array([170, 120, 120])
            upper_red2 = np.array([180, 255, 255])
            mask2 = cv2.inRange(hsv, lower_red2, upper_red2)

            # 마스크 합치기
            mask = mask1 + mask2
            cv2.imwrite('mask.jpg', mask)

            # 마스크 개선
            kernel = np.ones((2, 2), np.uint8)
            mask = cv2.dilate(mask, kernel, iterations=1)

            # 마스크 적용
            red_only = cv2.bitwise_and(image, image, mask=mask)
            cv2.imwrite('red_only.jpg', red_only)

            # 그레이스케일 변환
            gray = cv2.cvtColor(red_only, cv2.COLOR_BGR2GRAY)

            # 이진화
            _, binary = cv2.threshold(gray, 50, 255, cv2.THRESH_BINARY)
            cv2.imwrite('binary.jpg', binary)

            # OCR 수행
            config = '--psm 6 -c tessedit_char_whitelist=0123456789:'
            text = pytesseract.image_to_string(binary, config=config).strip()

            return text

        except Exception as e:
            print(f"OCR 에러: {str(e)}")
            return None

    def closeEvent(self, event):
        self.cap.release()


if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = DigitalClockOCR()
    ex.show()
    sys.exit(app.exec_())
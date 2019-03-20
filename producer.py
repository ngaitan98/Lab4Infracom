import sys
import time
import cv2
from kafka import KafkaProducer

topic = "streaming"

def publish_video(video_file):
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    video = cv2.VideoCapture(video_file)
    
    print('publishing video...')
    while(video.isOpened()):
        success, frame = video.read()
        if not success:
            print("bad read!")
            break        
        ret, buffer = cv2.imencode('.jpg', frame)
        producer.send(topic, buffer.tobytes())

        time.sleep(0.2)
    video.release()
    print('publish complete')

if __name__ == '__main__':
    video_path = 'y2mate.com - stranger_things_season_3_official_trailer_hd_netflix_YEG3bmU_WaI_1080p.mp4'
    publish_video(video_path)

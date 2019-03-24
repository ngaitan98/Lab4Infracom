import sys
import time
import cv2
from kafka import KafkaProducer


def publish_video(video_file, ip):
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    topic = ip
    video = cv2.VideoCapture(video_file)
    
    print('publishing video... for client ' + topic)
    while(video.isOpened()):
        success, frame = video.read()
        if not success:
            print("bad read!")
            break        
        ret, buffer = cv2.imencode('.jpg', frame)
        byteArray = buffer.tobytes()
        time.sleep(0.01)
        producer.send(topic, byteArray)

    video.release()
    print('publish complete')

if __name__ == '__main__':
    video_path = 'The 30-Second Video.mp4'
    publish_video(video_path, sys.argv[1])

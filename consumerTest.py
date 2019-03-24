import datetime
import os
from flask import Flask, Response, request
from kafka import KafkaConsumer
from threading import Thread


# Set the consumer in a Flask App
app = Flask(__name__)

@app.route('/video', methods=['GET'])
def video():
	topic = str(request.remote_addr)
	print("Nuevo cliente: " + topic)
	Thread(target = executeProducer, args = [topic]).start()
	print('Disfrute pez')
	consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'])
	return Response(get_video_stream(consumer), mimetype='multipart/x-mixed-replace; boundary=frame')

def get_video_stream(consumer):
	for msg in consumer:
		yield (b'--frame\r\n'
			b'Content-Type: image/jpg\r\n\r\n' + msg.value + b'\r\n\r\n')
def executeProducer(topic):
	os.system("python producer.py " + topic)

if __name__ == "__main__":
	app.run(host='0.0.0.0', debug=True)

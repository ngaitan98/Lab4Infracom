import datetime
import os
from flask import Flask, Response, request
from kafka import KafkaConsumer
from threading import Thread

app = Flask(__name__)

maxSupport = 160
clients = []

class Client:
	def __init__(self, ip):
		self.ip = ip
@app.route('/video', methods=['GET'])
def video():
	topic = str(request.remote_addr)
	timesVisited =countClientsByIp(topic)
	if len(clients) > maxSupport:
		print("Try Again Later")
		return
	newConnection = Client(topic)
	clients.append(newConnection)
	topic = topic + str(timesVisited + 1)
	print("New Client: " + topic)
	Thread(target = executeProducer, args = [topic]).start()
	consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'])
	return Response(get_video_stream(consumer,newConnection), mimetype='multipart/x-mixed-replace; boundary=frame')

def get_video_stream(consumer, client):
	for msg in consumer:
		yield (b'--frame\r\n'
			b'Content-Type: image/jpg\r\n\r\n' + msg.value + b'\r\n\r\n')
	clients.remove(client)
	print("Bye, bye " + client.ip)

def executeProducer(topic):
	os.system("python producer.py " + topic)

def countClientsByIp(ip):
	count = 0
	for client in clients:
		if client.ip == ip:
			count = count + 1
	return count
if __name__ == "__main__":
	app.run(host='0.0.0.0', debug=True)

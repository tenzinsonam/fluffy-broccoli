import datetime

import sys
import socket

PORT = 24000

s = socket.socket()
s.connect(('127.0.0.1', PORT))
print("Enter movie title: ")
inp = input()

s.send((inp).encode('UTF-8'))
time1 = datetime.datetime.now()

rec = (s.recv(1024)).decode('UTF-8')
time2 = datetime.datetime.now()

print(rec)
print("RoundTrip time "+str(time2-time1))
s.close()

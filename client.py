import sys
import socket

PORT = 24000

s = socket.socket()
s.connect(('localhost', PORT))
print("Enter movie title: ")
inp = input()
s.send((inp).encode('UTF-8'))
rec = (s.recv(1024)).decode('UTF-8')
print(rec)
s.close()

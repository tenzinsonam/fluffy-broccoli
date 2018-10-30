import json
import sys
import _mysql
from pymemcache.client import base
from pymemcache.client.hash import HashClient
import socket

PORT = 24001
TTL = 600

def setMessage(inp):
    str1 = str(len(inp))
    while(len(str1)<10):
        str1 = "0" + str1
    if(isinstance(inp,str)):
        return str1 + inp
    else:
        return str1.encode('UTF-8') + inp

def getMessage(s):
    rawn = s.recv(10).decode('UTF-8')
    if(len(rawn)==0)
        return ""
    return s.recv(int(rawn)).decode('UTF-8')

# Don't forget to run 'memcached' before running this next line:
memc = base.Client(('127.0.0.1',11211));
'''
memc = HashClient(
    ('localhost', 11211),
    ('localhost', 11212)
])
'''
conn = _mysql.connect (host = "localhost",
                        user = "root",
                        passwd = "password",
                        db = "cs632")

s = socket.socket()
s.bind(('', PORT))
s.listen(5)

# Request to be processed
while True:
    c, addr = s.accept()
    req = getMessage(c)
    req = json.loads(req)

    if req['query']=='UserExists?':
        req_value = req['value']+'#0'
        popularfilms = memc.get(req_value)
        if not popularfilms:
            print(req)
            qu = "SELECT userhash FROM status WHERE userhash='" + req_value +"'"
            conn.query(qu)
            rows = conn.store_result()
            rows = rows.fetch_row(how=1,maxrows=0)

            if(len(rows)!=0):
                c.sendall(setMessage(json.dumps({'code':0,'response':'User Already Exists\n'})).encode('UTF-8'))
            else:
                memc.set(req_value,'0',TTL)
                qu = "INSERT INTO status (userhash,message) VALUES ('" + req_value +"','0')"
                conn.query(qu)
                c.sendall(setMessage(json.dumps({'code':1,'response':'User Created Successfully'})).encode('UTF-8'))
           #print("Updated memcached with MySQL data")
        else:
            print("Loaded data from memcached")
            c.sendall(setMessage(json.dumps({'code':0,'response':'User Already Exists\n'})).encode('UTF-8'))
        c.close()

import sys
import _mysql
from pymemcache.client import base
from pymemcache.client.hash import HashClient
import socket

PORT = 24000
memc = base.Client(('127.0.0.1',11211));
'''
memc = HashClient([
    ('localhost', 11211),
    ('localhost', 11212)
])
'''
conn = _mysql.connect (host = "localhost",
                        user = "tenzin",
                        passwd = "tenzin",
                        db = "cs632")

s = socket.socket()
s.bind(('', PORT))
s.listen(5)
while True:
    c, addr = s.accept()
    #print(c)
    print(addr)
    req = (c.recv(1024)).decode('UTF-8')
    print(req)
    #assert False
    popularfilms = memc.get(req)
    if not popularfilms:
        print(req)
        qu = "SELECT rental_rate FROM filmorder WHERE title='" + req +"'"
        print(qu)
        conn.query(qu)
        rows = conn.store_result()
        rows = rows.fetch_row(how=1,maxrows=0)
        #for x in ro
        #print(rows)
        #assert False
        memc.set(req,rows,60)
        c.send((rows[0]['rental_rate']).encode('UTF-8'))
        print("Updated memcached with MySQL data")
    else:
        print("Loaded data from memcached")
        c.send((popularfilms))
        #print(popularfilms.decode('UTF-8'))
    c.close()

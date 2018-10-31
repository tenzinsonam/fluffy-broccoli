import json
import os
import sys
import _mysql
import traceback
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
    if(len(rawn)==0):
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
                        user = "tenzin",
                        passwd = "tenzin",
                        db = "cs632")

s = socket.socket()
s.bind(('', PORT))
s.listen(5)

# Request to be processed
while True:

    c, addr = s.accept()
    if not os.fork():
        s.close()
        try:
            req = getMessage(c)
            print(req)
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
                    print(rows)
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
                #dont change req
                reqnxt = getMessage(c)
                print(reqnxt)
                reqnxt = json.loads(reqnxt)
                if reqnxt['query']=='searchUser':
                    req_value = reqnxt['name'] + "#0"
                    popularfilms = memc.get(req_value)
                    if not popularfilms:
                        print("query from db")
                        qu = "SELECT message FROM status WHERE userhash='" + req_value +"'"
                        conn.query(qu)
                        rows = conn.store_result()
                        rows = rows.fetch_row(how=1,maxrows=0)
                        memc.set(req_value, (rows[0]['message']).decode('UTF-8'), TTL)
                        #memc.set(req_value,'0',TTL)
                    else:
                        print("query from memc")
                    #req_value = req['value'] + "#0"
                    tweets = int((memc.get(req_value)).decode('UTF-8'))
                    retstr = ""
                    print(tweets)
                    print(int(reqnxt['num']))
                    if(tweets< int(reqnxt['num'])):
                        while tweets>0:
                            tweet = reqnxt['name'] +'#'+ str(tweets)
                            #inmemc = (memc.get(tweet)).decode('UTF-8')
                            inmemc = (memc.get(tweet))
                            if not inmemc:
                                qu = "SELECT message FROM status WHERE userhash='" + tweet +"'"
                                conn.query(qu)
                                rows = conn.store_result()
                                rows = rows.fetch_row(how=1,maxrows=0)
                                memc.set(tweet, (rows[0]['message']).decode('UTF-8'), TTL)
                                retstr+= (rows[0]['message']).decode('UTF-8') + '\n'
                            else:
                                inmemc = (memc.get(tweet)).decode('UTF-8')
                                retstr += inmemc + '\n'
                            tweets-=1
                    else:
                        #retstr = ""
                        numofrec = int(reqnxt['num'])
                        while numofrec >0:
                            tweet = reqnxt['name'] +'#'+ str(tweets)
                            #inmemc = (memc.get(tweet)).decode('UTF-8')
                            inmemc = (memc.get(tweet))
                            if not inmemc:
                                qu = "SELECT message FROM status WHERE userhash='" + tweet +"'"
                                conn.query(qu)
                                rows = conn.store_result()
                                rows = rows.fetch_row(how=1,maxrows=0)
                                memc.set(tweet, (rows[0]['message']).decode('UTF-8'), TTL)
                                retstr+= (rows[0]['message']).decode('UTF-8') + '\n'
                                print("message is" + (rows[0]['message']).decode('UTF-8') + '\n')
                            else:
                                inmemc = (memc.get(tweet)).decode('UTF-8')
                                retstr += inmemc + '\n'
                            numofrec-=1
                            tweets-=1
                    print(retstr)

                    c.sendall(setMessage((retstr).encode('UTF-8')))
                elif reqnxt['query']=='updateUserinfo':
                    req_value = req['value'] + "#0"
                    popularfilms = memc.get(req_value)
                    if not popularfilms:
                        print("query from db")
                        qu = "SELECT * FROM status WHERE userhash='" + req_value +"'"
                        conn.query(qu)
                        rows = conn.store_result()
                        rows = rows.fetch_row(how=1,maxrows=0)
                        print(rows)
                        memc.set(req_value, (rows[0]['message']).decode('UTF-8'), TTL)
                        #memc.set(req_value,'0',TTL)
                    else:
                        print("query from memc")
                    tweets = int((memc.get(req_value)).decode('UTF-8'))
                    print("tweets: " + str(tweets))
                    qu = "INSERT INTO status (userhash,message) VALUES ('" + req['value']+"#"+str(tweets+1) +"','"+reqnxt['value']+"')"
                    conn.query(qu)
                    c.sendall(setMessage(json.dumps({'code':1,'response':'Status updated'})).encode('UTF-8'))
                    memc.set(req_value, tweets+1, TTL)
                    qu = "UPDATE status SET message='" +str(tweets+1) +"' WHERE userhash='"+req_value +"';"
                    conn.query(qu)


        except Exception as e:
            print(str(e))
            traceback.print_exc()
        finally:
            c.close()
            exit(0)

    else:
        c.close()

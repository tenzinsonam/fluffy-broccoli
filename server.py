import json
import os
import sys
import _mysql
import traceback
from pymemcache.client import base
from pymemcache.client.hash import HashClient
import socket

debug = True

PORT = 24002
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

def createDatabaseConn():
    conn = _mysql.connect (host = "localhost",
                            user = "user",
                            passwd = "password",
                            db = "cs632")
    return conn

#<<<<<<< HEAD




#=======
#>>>>>>> ea7ac77db1c76406e0b416437f795312c42342cd
#check it brotha line 134
# Don't forget to run 'memcached' before running this next line:
memc = base.Client(('127.0.0.1',11211));
'''
memc = HashClient(
    ('localhost', 11211),
    ('localhost', 11212)
])
'''


s = socket.socket()
s.bind(('', int(sys.argv[1])))
s.listen(5)

# Request to be processed
while True:

    c, addr = s.accept()
    if not os.fork():
        s.close()
        try:
            conn = createDatabaseConn()
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
                        c.sendall(setMessage(json.dumps({'code':0,'response':'User Exists\n'})).encode('UTF-8'))
                    elif 'createUserIfNotExists' in req.keys() and req['createUserIfNotExists'] == False:
                        c.sendall(setMessage(json.dumps({'code':1,'response':'User doesn\'t Exists\n'})).encode('UTF-8'))
                    else:
                        memc.set(req_value,'0',TTL)
                        qu = "INSERT INTO status (userhash,message) VALUES ('" + req_value +"','0')"
                        conn.query(qu)
                        c.sendall(setMessage(json.dumps({'code':1,'response':'User Created Successfully'})).encode('UTF-8'))
                   #print("Updated memcached with MySQL data")
                else:
                    print("Loaded data from memcached")
                    c.sendall(setMessage(json.dumps({'code':0,'response':'User Exists\n'})).encode('UTF-8'))
                #dont change req
                # req = getMessage(c)
                # print(req)
                # req = json.loads(req)

            if req['query']=='searchUser':
                print("art thou in search")
                req_value = req['name'] + "#0"
                num_str = (memc.get(req_value)).decode('UTF-8')
                if not num_str:
                    if debug:
                        print("query from db")
                    conn.query("SELECT message FROM status WHERE userhash='" + req_value +"'")
                    rows = conn.store_result().fetch_row(how=1,maxrows=0)

                    #TODO
                    # Set the TTL appropriately
                    memc.set(req_value, (rows[0]['message']).decode('UTF-8'), TTL)
                    num_str = rows[0]['message']
                else:
                    if(debug):
                        print("query from memc")
                tweets = int(num_str)#.decode('UTF-8')
                retstr = ""

                if(debug):
                    print(tweets)
                    print(int(req['num']))

                user_postno = []
                if 'post_nos' in req.keys():
                    for i in req['post_nos']:
                        user_postno.append(req['name']+'#'+str(i))
                else:
                    requested_numposts = int(req['num'])
                    while tweets < 1 or requested_numposts < 1:
                        user_postno.append(req['name']+'#'+tweets)
                        tweets -=1
                        requested_numposts -=1

                #check it brotha
                user_posts = get_many(user_postno)

                missed_posts = []
                for key,value in user_posts.items():
                    if value == None:
                        missed_posts.append(key)

                if len(missed_posts)<0:
                    db_req_keys = "'"+str(missed_posts)+"'"
                    for i in range(1,len(missed_posts)):
                        db_req_keys += ","+"'"+str(missed_posts[i])+"'"
                    db_req_keys = "("+db_req_keys+")"
                    conn.query("SELECT message FROM status WHERE userhash in " +db_req_keys +"")
                    rows = conn.store_result().fetch_row(how=1,maxrows=0)
                    temp_keyvalue = {}
                    for row in rows:
                        temp_keyvalue[row['userhash']] = temp_keyvalue[row['message']]
                        user_posts[row['userhash']] = user_posts[row['message']]
                    memc.set(temp_keyvalue)

                retstr = ""
                for key,value in user_posts.items():
                    retstr = key + " --> " + value
                if(debug):
                    print(retstr)
                c.sendall(setMessage((retstr).encode('UTF-8')))

            if req['query']=='updateUserinfo':
                print('update')
                req_value = req['name'] + "#0"
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
                qu = "INSERT INTO status (userhash,message) VALUES ('" + req['value']+"#"+str(tweets+1) +"','"+req['value']+"')"
                conn.query(qu)
                c.sendall(setMessage(json.dumps({'code':1,'response':'Status updated'})).encode('UTF-8'))
                memc.set(req_value, tweets+1, TTL)
                qu = "UPDATE status SET message='" +str(tweets+1) +"' WHERE userhash='"+req_value +"';"
                conn.query(qu)

            if req['query'] =='deleteUser':
                userZero = req['name'] +'#0'
                memcacheUserZero = memc.get(userZero)
                if memcacheUserZero:
                    memc.set(userZero, -1*memcacheUserZero, TTL)
                print("query from db")
                qu = "SELECT * FROM status WHERE userhash='" + userZero +"';"
                conn.query(qu)
                rows = conn.store_result()
                rows = rows.fetch_row(how=1, maxrows=0)
                tweetnum = int((rows[0]['message']).decode('UTF-8'))
                print(tweetnum)
                qu = "UPDATE status SET message='" + str(-1*tweetnum)  + "' WHERE userhash='"+userZero+"';"
                conn.query(qu)
                c.sendall(setMessage(json.dumps({'code':1, 'response':"User deleted"})).encode('UTF-8'))
                qu = "DELETE FROM status WHERE userhash LIKE '" + userZero[:-1] + "%';"
                conn.query(qu)

                '''
                elif reqnxt['query']=='deleteComm':
                    userZero = req['value'] + '#0'
                    memcacheUserZero = int((memc.get(userZero)).decode('UTF-8'))
                    if not memcacheUserZero:

                    else:
                        memc.set()

                '''



        except Exception as e:
            print(str(e))
            traceback.print_exc()
        finally:
            c.close()
            exit(0)

    else:
        c.close()

import json
import os
import sys
import _mysql
import traceback
import math
import datetime
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
                breakLoop = False 
                while not breakLoop:
                    req_value = req['value']+'#0'
                    popularfilms, cas = memc.gets(req_value)
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
                            #memc.set(req_value,'0',TTL)
                            if not memc.cas(req_value,'0',cas,expire=TTL):
                                continue
                            qu = "INSERT INTO status (userhash,message,expires) VALUES ('" + req_value +"','0', -1)"
                            conn.query(qu)
                            c.sendall(setMessage(json.dumps({'code':1,'response':'User Created Successfully'})).encode('UTF-8'))
                       #print("Updated memcached with MySQL data")
                    else:
                        print("Loaded data from memcached")
                        c.sendall(setMessage(json.dumps({'code':0,'response':'User Exists\n'})).encode('UTF-8'))
                    breakLoop = True
                    #dont change req
                    # req = getMessage(c)
                    # print(req)
                    # req = json.loads(req)

            if req['query']=='searchUser':
                print("art thou in search")
                req_value = req['name'] + "#0"
                num_str = (memc.get(req_value))
                if not num_str:
                    if debug:
                        print("query from db")
                    conn.query("SELECT * FROM status WHERE userhash='" + req_value +"'")
                    rows = conn.store_result().fetch_row(how=1,maxrows=0)

                    #TODO if the search user doesn't exist in the database

                    #TODO
                    # Set the TTL appropriately
                    memc.set(req_value, (rows[0]['message']).decode('UTF-8'), TTL)
                    num_str = rows[0]['message']
                else:
                    if(debug):
                        print("query from memc")
                tweets = int(num_str.decode('UTF-8'))
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
                    while tweets >= 1 and requested_numposts >= 1:
                        user_postno.append(req['name']+'#'+str(tweets))
                        tweets -=1
                        requested_numposts -=1

                #check it brotha
                user_posts = memc.get_many(user_postno)

                print(user_posts)
                missed_posts = []
                for key in user_postno:
                    if key not in user_posts.keys():
                        missed_posts.append(key)

                print('Missed posts ->' +str(missed_posts))
                if len(missed_posts)>0:
                    db_req_keys = "'"+str(missed_posts[0])+"'"
                    for i in range(1,len(missed_posts)):
                        db_req_keys += ","+"'"+str(missed_posts[i])+"'"
                    db_req_keys = "("+db_req_keys+")"
                    conn.query("SELECT userhash,message FROM status WHERE userhash in " +db_req_keys +"")
                    rows = conn.store_result().fetch_row(how=1,maxrows=0)
                    temp_keyvalue = {}
                    for row in rows:
                        print(row)
                        #TODO: check for xpiry
                        temp_keyvalue[row['userhash']] = row['message']
                        user_posts[row['userhash']] = row['message']
                    memc.set_many(temp_keyvalue)


                retstr = ""
                for key,value in user_posts.items():
                    retstr += str(key) + " --> " + str(value.decode('UTF-8'))+"\n"
                print(retstr)
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
                qu = "INSERT INTO status (userhash,message, expires) VALUES ('" + req['name']+"#"+str(tweets+1) +"','"+req['value']+"',-1)"
                conn.query(qu)
                c.sendall(setMessage(json.dumps({'code':1,'response':'Status updated'})).encode('UTF-8'))
                memc.set(req_value, tweets+1, TTL)
                qu = "UPDATE status SET message='" +str(tweets+1) +"' WHERE userhash='"+req_value +"';"
                conn.query(qu)

            if req['query'] =='deleteUser':
                userZero = req['name'] +'#0'
                memcacheUserZero = memc.get(userZero)
                if memcacheUserZero:
                    memc.set(userZero, -1*int((memcacheUserZero).decode('UTF-8')), TTL)
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


            #TODO tricky #0 updates
            if req['query']=='deletePosts':
                userZero = req['name']+'#0'
                memcacheUserZero = memc.get(userZero)   #used later
                print('query from db')
                qu = "SELECT * FROM status WHERE userhash='" +userZero+"';"
                conn.query(qu)
                rows = conn.store_result();
                rows = rows.fetch_row(how=1, maxrows=0)
                tweetnum = int((rows[0]['message']).decode('UTF-8'))
                if req['value'].find('-')!=-1:
                    deleteRange = req['value'].split('-')
                    deleteRange = [int(x) for x in deleteRange]
                    negativeDict = {}
                    for x in range(math.ceil(deleteRange[0]/100), math.ceil((deleteRange[1]/100))+1):
                        negativeDict[str(x)] = []
                        qu = "SELECT * FROM status WHERE userhash='" + userZero[:-1] +str(-1*x)+"';"
                        conn.query(qu)
                        rows = conn.store_result()
                        rows = rows.fetch_row(how=1, maxrows=0)
                        if(len(rows)==0):
                            qu = "INSERT INTO status (userhash, message, expires) VALUES ('" + userZero[:-1]+str(-1*x) +"','',-1);"
                            conn.query(qu)
                        else:
                            negativeDict[str(x)].append(rows[0]['message'].decode('UTF-8'))

                    deleteRangeExpanded = []
                    for x in range(deleteRange[0], deleteRange[1]+1):
                        deleteRangeExpanded.append(str(x))
                        qu = "DELETE FROM status WHERE userhash='"+userZero[:-1] +str(x) +"';"
                        negativeDict[str(math.ceil(x/100))].append(str(x))
                        conn.query(qu)
                    memc.delete_many(deleteRangeExpanded)

                    #update #0
                    #TODO cehck if value of #0 goes out of range
                    if tweetnum>=deleteRange[0] and tweetnum<=deleteRange[1]:
                        inRange = math.ceil(tweetnum/100)
                        tempList = (",".join(negativeDict[str(inRange)])).split(',')
                        while(len(tempList)==100):
                            inRange-=1
                            #if str(inRange)
                            tempList = (",".join(negativeDict[str(inRange)])).split(',')
                        possibleValue = inRange*100
                        for x in range(inRange*100, (inRange-1)*100, -1):
                            if x not in tempList:
                                possibleValue=x

                        qu = "UPDATE status SET message='" +str(possibleValue)+  "' WHERE userhash='" + userZero+"';"
                        conn.query(qu)



                        if memcacheUserZero:
                            memc.set(userZero,str(possibleValue) , TTL)
                    for x in range(math.ceil(deleteRange[0]/100), math.ceil((deleteRange[1]/100))+1):
                        qu = "UPDATE status SET message='" + ",".join(negativeDict[str(x)])+"' WHERE userhash='" +userZero[:-1]+str(-1*x)+"';"
                        conn.query(qu)

                else:
                    deleteRow = int(req['value'])
                    negativeDict = {}
                    qu = "SELECT * FROM status WHERE userhash='" + userZero[:-1] +str(-1*math.ceil(deleteRow/100))+"';"
                    negativeDict[str(math.ceil(deleteRow/100))] = []
                    conn.query(qu)
                    rows = conn.store_result()
                    rows = rows.fetch_row(how=1, maxrows=0)
                    if(len(rows)==0):
                        qu = "INSERT INTO status (userhash, message, expires) VALUES ('" + userZero[:-1] +str(-1*math.ceil(deleteRow/100)) +"','',-1);"
                        conn.query(qu)
                    else:
                        negativeDict[str(math.ceil(deleteRow/100))].append(rows[0]['message'].decode('UTF-8'))
                    negativeDict[str(math.ceil(deleteRow/100))].append(str(deleteRow))
                    qu = "DELETE FROM status WHERE userhash='"+userZero[:-1] +str(deleteRow) +"';"
                    conn.query(qu)

                    if tweetnum==deleteRow:
                        inRange = math.ceil(tweetnum/100)
                        tempList = (",".join(negativeDict[str(inRange)])).split(',')
                        while(len(tempList)==100):
                            inRange-=1
                            i#f str(inRange)
                            tempList = (",".join(negativeDict[str(inRange)])).split(',')
                        possibleValue = inRange*100
                        for x in range(inRange*100, (inRange-1)*100, -1):
                            if x not in tempList:
                                possibleValue=x

                        qu = "UPDATE status SET message='" +str(possibleValue)+  "' WHERE userhash='" + userZero+"';"
                        conn.query(qu)



                        if memcacheUserZero:
                            memc.set(userZero,str(possibleValue) , TTL)


                    qu = "UPDATE status SET message='" +",".join(negativeDict[str(math.ceil(deleteRow/100))]) +"' WHERE userhash='" +userZero[:-1] +str(-1*math.ceil(deleteRow/100))+"';"
                    conn.query(qu)

                c.sendall(setMessage(json.dumps({'code':1, 'response':"User posts deleted"})).encode('UTF-8'))


            if req['query']=='updateTill':
                userZero = req['name']+'#0'
                #newTTL = req['time']
                req_value = req['name'] + "#0"
                timenow = datetime.datetime.now()
                #deltaParam = "=".join()
                deltaParam =[int(req['time'][2]),int(req['time'][1]),int(req['time'][0]),0]
                print(deltaParam)
                newTTL = (timenow + datetime.timedelta(hours=deltaParam[0], minutes=deltaParam[1], seconds=deltaParam[2])).strftime("%Y-%m-%d %H:%M:%S")
                memcacheUserZero = memc.get(userZero)
                if not memcacheUserZero:
                    print("query from db")
                    qu = "SELECT * FROM status WHERE userhash='" + userZero +"'"
                    conn.query(qu)
                    rows = conn.store_result()
                    rows = rows.fetch_row(how=1,maxrows=0)
                    print(rows)
                    #memc.set(req_value, (rows[0]['message']).decode('UTF-8'), int(newTTL))
                    memcacheUserZero = rows[0]['message']
                else:
                    print("query from memc")
                tweets = int(memcacheUserZero.decode('UTF-8'))
                qu = "INSERT INTO status (userhash,message,expires) VALUES ('" + req['name']+"#"+str(tweets+1) +"','"+req['value']+"','" +str(newTTL)+"');"
                conn.query(qu)
                c.sendall(setMessage(json.dumps({'code':1,'response':'Status updated'})).encode('UTF-8'))
                memc.set(userZero, tweets+1, newTTL)
                print(datetime.datetime.now())
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

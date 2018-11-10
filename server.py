import json
import time
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

def createDatabaseConn2():
    conn2 = _mysql.connect (host = "localhost",
                            user = "user",
                            passwd = "password",
                            db = "cs632")
    return conn2

#userLock = {}
#username is the key
#the function will return false if currently resource is locked
#so place it in while loop
#input conn2
def  addMemcache(username, conn, memc):
    #userLock[username] = "free"
    while not memc.add(username, free,999999):
        pass
    qu = "SELECT * FROM lockingDict WHERE userhash='" +username+ "';"
    conn.query(qu)
    rows = conn.store_result()
    rows = rows.fetch_row(how=1, maxrows=0)
    if(len(rows)==0):
        qu = "INSERT INTO lockingDict (userhash, lock) VALUES ('" +username+"', lock);"
        conn.query(qu)
    else:
        lockStatus = rows[0]['lock']
        if(lockStatus==lock):
            return False
        qu = "UPDATE lockingDict SET lock='lock' WHERE userhash='" +username+ "';"
        conn.query(qu)
    return True
    #return conn

def delMemcache(username, conn, memc):
    memc.delete(username)
    qu = "UPDATE lockingDict SET lock='free' WHERE userhash='" +username+ "';"
    conn.query(qu)

def setLatest(memcLatest, message, timeString):
    if not (memcLatest.get("Latest")):
        memcLatest.add("latest#"+timeString, message, 20)
    else:
        memcLatest.append("latest#"+timeString, "^^^^"+message, 20)


#<<<<<<< HEAD

def getmemc():
    return base.Client(('127.0.0.1',11211));

def attemptLock(conn,memc,username):
    response = memc.get('#status:'+username+':1') # response denotes if the lock exists
    if response is None:
        print('Lock not found in memcached')
        ar = conn.query('update locks set status=1 where user="'+str(username)+'" and status=0;')
        if conn.affected_rows() == 0:
            print('Lock already in db')
            return False
        else:
            print('Lock acquired in db')
            #if memc.cas(key = '#status:'+username+':1',value = 1,cas = cas):
            if memc.add('#status:'+username+':1',1):
                return True
            else:
                print("Something conflicted with cache, lockattempt back-offed")
                conn.query('update locks set status=0 where user="'+str(username)+'" and status=1;')
                return False
    else:
        print('Lock in memcached')
        return False
    '''
    elif int(response)==1:
        return False
    elif int(response)==0:
        if memc.add(response,1,cas):
    '''

def releaseLock(conn,memc,username):
    response = memc.get('#status:'+username+':1')
    if response is None:
        ## Lock entry got evicted from Memcached
        pass
    else:
        memc.delete('#status:'+username+':1')
    conn.query('update locks set status=0 where user="'+str(username)+'";')



#                        conn.query(qu)
#                        rows = conn.store_result()
#                        rows = rows.fetch_row(how=1,maxrows=0)


#=======
#>>>>>>> ea7ac77db1c76406e0b416437f795312c42342cd
#check it brotha line 134
# Don't forget to run 'memcached' before running this next line:
'''
memc = HashClient(
    ('localhost', 11211),
    ('localhost', 11212)
])
'''


s = socket.socket()
#memcLatest.add("Latest", "", 20)
s.bind(('', int(sys.argv[1])))
s.listen(5)
memc = getmemc()
memc.flush_all()
# Request to be processed
while True:

    c, addr = s.accept()
    if not os.fork():
        s.close()
        try:
            memc = getmemc()
            conn = createDatabaseConn()
            conn2 = createDatabaseConn2()
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
                            #if not memc.cas(key = req_value,value = '0',cas = cas):
                            #    continue
                            if not memc.add(key = req_value,value='0'):
                                continue
                            qu = "INSERT INTO status (userhash,message,expires) VALUES ('" + req_value +"','0', -1);"
                            conn.query(qu)
                            qu = "INSERT INTO locks (user,status) VALUES ('" + req['name']+ "','0')"
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
                conn.query('select * from locks where user = "'+str(req['name'])+'";');
                rows = conn.store_result()
                rows = rows.fetch_row(how=1,maxrows=0)
                if len(rows)==0:
                        c.sendall(setMessage(('User doesn\'t exist in the Lock database. Fetch failed.').encode('UTF-8')))
                        c.close()
                        continue

                while not attemptLock(conn,memc,req['name']):
                    time.sleep(0.001)

                try:
                    print("art thou in search")
                    req_value = req['name'] + "#0"
                    num_str = (memc.get(req_value))
                    if not num_str:
                        if debug:
                            print("query from db")
                        conn.query("SELECT * FROM status WHERE userhash='" + req_value +"';")
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

                    # if(debug):
                    #     print(tweets)
                    #     print(int(req['num']))
                    #
                    # user_postno = []
                    # if 'post_nos' in req.keys():
                    #     for i in req['post_nos']:
                    #         user_postno.append(req['name']+'#'+str(i))
                    # else:
                    #     requested_numposts = int(req['num'])
                    #
                    #     while tweets >= 1 and requested_numposts >= 1:
                    #         user_postno.append(req['name']+'#'+str(tweets))
                    #         tweets -=1
                    #         requested_numposts -=1
                    #
                    # #check it brotha
                    # user_posts = memc.get_many(user_postno)
                    #
                    #         #TODO if the search user doesn't exist in the database
                    #
                    # print('Missed posts ->' +str(missed_posts))
                    # if len(missed_posts)>0:
                    #     db_req_keys = "'"+str(missed_posts[0])+"'"
                    #     for i in range(1,len(missed_posts)):
                    #         db_req_keys += ","+"'"+str(missed_posts[i])+"'"
                    #     db_req_keys = "("+db_req_keys+")"
                    #     conn.query("SELECT * FROM status WHERE userhash in " +db_req_keys +"")
                    #     rows = conn.store_result().fetch_row(how=1,maxrows=0)
                    #     temp_keyvalue = {}
                    #     for row in rows:
                    #         print(row)
                    #         #TODO: check for xpiry
                    #         temp_keyvalue[row['userhash']] = row['message']
                    #         user_posts[row['userhash']] = row['message']
                    #     memc.set_many(temp_keyvalue)

                    if(debug):
                        print(tweets)
                        print(int(req['num']))

                    user_postno = []
                    if 'post_nos' in req.keys():
                        for i in req['post_nos']:
                            user_postno.append(req['name']+'#'+str(i))
                    else:

                        requested_numposts = int(req['num'])

                        while tweets > 0 and requested_numposts > 0:
                            del_list = []
                            deleted = str(req['name'])+'#-'+ str((int((tweets-1)/100)+1))
                            del_postnos = memc.get(deleted)
                            if not del_postnos:
                                qu = "SELECT userhash FROM status WHERE userhash='" + deleted +"';"
                                conn.query(qu)
                                rows = conn.store_result()
                                rows = rows.fetch_row(how=1,maxrows=0)
                                if len(rows)==0:
                                    ## No rows for that range is deleted
                                    del_postnos=""
                                    pass
                                else:
                                    del_postnos = rows[0]['userhash']
                            del_postnos = (del_postnos.decode('UTF-8')).split(',')
                            for i in del_postnos:
                                try:
                                    del_list.append(int(i))
                                except:
                                    continue


                            while tweets >= 1 and requested_numposts >= 1:
                                if tweets not in del_list:
                                    user_postno.append(req['name']+'#'+str(tweets))
                                    requested_numposts -=1
                                tweets -=1
                                if tweets%100 ==0:
                                    break

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
                            conn.query("SELECT userhash,message FROM status WHERE userhash in " +db_req_keys +";")
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

                except:
                    print(traceback.format_exc())
                    print(sys.exc_info()[0])
                finally:
                    releaseLock(conn,memc,req['name'])



            if req['query']=='updateUserinfo':
                while not attemptLock(conn,memc,req['name']):
                    time.sleep(0.001)

                try:
                    print('update')
                    req_value = req['name'] + "#0"
                    print(req_value)
                    popularfilms = memc.get(req_value)
                    if not popularfilms:
                        print("query from db")
                        qu = "SELECT * FROM status WHERE userhash='" + req_value +"'"
                        conn.query(qu)
                        rows = conn.store_result()
                        rows = rows.fetch_row(how=1,maxrows=0)
                        print(rows)
                        memc.set(req_value, (rows[0]['message']).decode('UTF-8'), TTL)
                        popularfilms = rows[0]['message']
                        #memc.set(req_value,'0',TTL)
                    else:
                        print("query from memc")
                    tweets = int((popularfilms).decode('UTF-8'))
                    if tweets < 0:
                        c.sendall(setMessage(json.dumps({'code':1,'response':'Update Failed'})).encode('UTF-8'))
                        break
                    print("tweets: " + str(tweets))
                    qu = "INSERT INTO status (userhash,message, expires) VALUES ('" + req['name']+"#"+str(tweets+1) +"','"+req['value']+"',-1)"
                    conn.query(qu)
                    memc.set(req['name']+"#"+str(tweets+1), req['value'])
                    setLatest(memc, req['name']+"#"+str(tweets+1),(datetime.datetime.now()).strftime("%Y-%m-%d_%H:%M:%S"))
                    c.sendall(setMessage(json.dumps({'code':1,'response':'Status updated'})).encode('UTF-8'))
                    memc.set(req_value, tweets+1, TTL)
                    qu = "UPDATE status SET message='" +str(tweets+1) +"' WHERE userhash='"+req_value +"';"
                    conn.query(qu)

                except:
                    #if e != "":
                    print(traceback.format_exc())
                    print(sys.exc_info()[0])
                finally:
                    releaseLock(conn,memc,req['name'])

            if req['query'] =='deleteUser':
                while not attemptLock(conn,memc,req['name']):
                    time.sleep(0.001)

                try:
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
                    qu = "DELETE FROM locks WHERE user = '"+req['name']+"';"
                    conn.query(qu)
                    if memcacheUserZero:
                        memc.delete(userZero)

                    '''
                    elif reqnxt['query']=='deleteComm':
                        userZero = req['value'] + '#0'
                        memcacheUserZero = int((memc.get(userZero)).decode('UTF-8'))
                        if not memcacheUserZero:

                        else:
                            memc.set()

                    '''

                except:
                    print(traceback.format_exc())
                    print(sys.exc_info()[0])
                finally:
                    # TODO
                    # I'm deleting USER FROM database, how do I release the lock
                    releaseLock(conn,memc,req['name'])



            if req['query']=='deletePosts':
                while not attemptLock(conn,memc,req['name']):
                    time.sleep(0.001)

                try:

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
                                negativeDict[str(x)].append((rows[0]['message'].decode('UTF-8')).strip())

                        deleteRangeExpanded = []
                        for x in range(deleteRange[0], deleteRange[1]+1):
                            deleteRangeExpanded.append(str(x))
                            qu = "DELETE FROM status WHERE userhash='"+userZero[:-1] +str(x) +"';"
                            negativeDict[str(math.ceil(x/100))].append(str(x))
                            tempList = (",".join(negativeDict[str(math.ceil(x/100))])).split(',')
                            tempList = list(set(tempList))
                            negativeDict[str(math.ceil(x/100))] = tempList
                            conn.query(qu)
                        memc.delete_many(deleteRangeExpanded)

                        #update #0
                        #TODO cehck if value of #0 goes out of range
                        if tweetnum>=deleteRange[0] and tweetnum<=deleteRange[1]:
                            inRange = math.ceil(tweetnum/100)
                            tempList = negativeDict[str(inRange)]
                            #tempList = list(set(tempList))
                            while(len(tempList)==100):
                                inRange-=1
                                #if str(inRange)
                                tempList = negativeDict[str(inRange)]
                                #tempList = list(set(tempList))
                            possibleValue = inRange*100
                            for x in range(inRange*100, (inRange-1)*100, -1):
                                if str(x) not in tempList and x <tweetnum:
                                    possibleValue=x
                                    break

                            qu = "UPDATE status SET message='" +str(possibleValue)+  "' WHERE userhash='" + userZero+"';"
                            conn.query(qu)

                            #for x in range(math.ceil(deleteRange[0]/100), inRange):
                            #    qu = "UPDATE status SET message='" + ",".join(negativeDict[str(x)])+"' WHERE userhash='" +userZero[:-1]+str(-1*x)+"';"
                            #    conn.query(qu)

                            # changing negative dict
                            inRangelist = []
                            for x in negativeDict[str(inRange)]:
                                if int(x) < possibleValue:
                                    inRangelist.append(x)
                            negativeDict[str(inRange)] = inRangelist
                            for x in range(inRange+1, math.ceil((deleteRange[1]/100))+1):
                                negativeDict[str(x)] = []
                            for x in range(math.ceil(deleteRange[0]/100), math.ceil((deleteRange[1]/100))+1):
                                qu = "UPDATE status SET message='" + ",".join(negativeDict[str(x)])+"' WHERE userhash='" +userZero[:-1]+str(-1*x)+"';"
                                conn.query(qu)




                            if memcacheUserZero:
                                memc.set(userZero,str(possibleValue) , TTL)
                        else:

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
                            negativeDict[str(math.ceil(deleteRow/100))].append((rows[0]['message'].decode('UTF-8')).strip())
                        negativeDict[str(math.ceil(deleteRow/100))].append(str(deleteRow))
                        tempList = (",".join(negativeDict[str(math.ceil(deleteRow/100))])).split(',')
                        tempList = list(set(tempList))
                        negativeDict[str(math.ceil(deleteRow/100))] = tempList
                        qu = "DELETE FROM status WHERE userhash='"+userZero[:-1] +str(deleteRow) +"';"
                        conn.query(qu)

                        if tweetnum==deleteRow:
                            inRange = math.ceil(tweetnum/100)
                            tempList = negativeDict[str(inRange)]
                            '''
                            while(len(tempList)==100):
                                inRange-=1
                                #i#f str(inRange)
                                tempList = (",".join(negativeDict[str(inRange)])).split(',')

                            '''
                            possibleValue = inRange*100
                            for x in range(inRange*100, (inRange-1)*100, -1):
                                if str(x) not in tempList and x < tweetnum:
                                    possibleValue=x
                                    break

                            qu = "UPDATE status SET message='" +str(possibleValue)+  "' WHERE userhash='" + userZero+"';"
                            conn.query(qu)



                            if memcacheUserZero:
                                memc.set(userZero,str(possibleValue) , TTL)

                            inRangelist = []
                            for x in negativeDict[str(inRange)]:
                                if int(x) < possibleValue:
                                    inRangelist.append(x)
                            negativeDict[str(inRange)] = inRangelist


                        qu = "UPDATE status SET message='" +",".join(negativeDict[str(math.ceil(deleteRow/100))]) +"' WHERE userhash='" +userZero[:-1] +str(-1*math.ceil(deleteRow/100))+"';"
                        conn.query(qu)

                    c.sendall(setMessage(json.dumps({'code':1, 'response':"User posts deleted"})).encode('UTF-8'))

                except:
                    print(traceback.format_exc())
                    print(sys.exc_info()[0])
                finally:
                    releaseLock(conn,memc,req['name'])



            if req['query']=='updateTill':
                while not attemptLock(conn,memc,req['name']):
                    time.sleep(0.001)

                try:

                    userZero = req['name']+'#0'
                    #TODO if userZero.value < 0 return False
                    #newTTL = req['time']
                    req_value = req['name'] + "#0"
                    timenow = datetime.datetime.now()
                    #deltaParam = "=".join()
                    deltaParam =[int(req['time'][2]),int(req['time'][1]),int(req['time'][0]),0]
                    print(deltaParam)
                    newTTL = (timenow + datetime.timedelta(hours=deltaParam[0], minutes=deltaParam[1], seconds=deltaParam[2])).strftime("%Y-%m-%d-%H:%M:%S")
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
                    if tweets < 0:
                        c.sendall(setMessage(json.dumps({'code':1,'response':'Update Failed'})).encode('UTF-8'))
                        break
                    qu = "INSERT INTO status (userhash,message,expires) VALUES ('" + req['name']+"#"+str(tweets+1) +"','"+req['value']+"','" +str(newTTL)+"');"
                    conn.query(qu)
                    setLatest(memc, req['name']+"#"+str(tweets+1),(datetime.datetime.now()).strftime("%Y-%m-%d_%H:%M:%S"))
                    memc.set(req['name']+"#"+str(tweets+1), req['value'])
                    c.sendall(setMessage(json.dumps({'code':1,'response':'Status updated'})).encode('UTF-8'))
                    memc.set(userZero, tweets+1, newTTL)
                    print(datetime.datetime.now())
                    qu = "UPDATE status SET message='" +str(tweets+1) +"' WHERE userhash='"+req_value +"';"
                    conn.query(qu)

                except:
                    print(traceback.format_exc())
                    print(sys.exc_info()[0])
                finally:
                    releaseLock(conn,memc,req['name'])

                    #TODO changes in latestString append mechanism
            if req['query']=="getlatest":
                timeNow = datetime.datetime.now()
                timeList = []
                for x in range(21):
                    timeCheck = (timeNow - datetime.timedelta(seconds=x)).strftime("%Y-%m-%d_%H:%M:%S")
                    userValue = memc.get("latest#"+str(timeCheck))
                    if userValue:
                        timeList.append(userValue.decode("UTF-8"))
                responseString=""
                for x in timeList:
                    x = x.split("^^^^")
                    for userHash in x:
                        num_str = (memc.get(userHash))
                        if not num_str:
                            conn.query("SELECT * FROM status WHERE userhash='" + userHash +"'")
                            rows = conn.store_result().fetch_row(how=1,maxrows=0)
                            memc.set(userHash, (rows[0]['message']).decode('UTF-8'), TTL)
                            num_str = rows[0]['message']
                        messAge = num_str.decode("UTF-8")
                        responseString += userHash + "---->" + messAge +"\n"
                print(timeList)
                if responseString!="":
                    c.sendall(setMessage(json.dumps({'code':1,'response':responseString})).encode('UTF-8'))
                else:
                    c.sendall(setMessage(json.dumps({'code':1,'response':'No latest messages present'})).encode('UTF-8'))

                '''
                latestString = memc.get("Latest")
                if not latestString:
                    c.sendall(setMessage(json.dumps({'code':1,'response':'No latest messages present'})).encode('UTF-8'))
                else:
                    latestString = latestString.decode('UTF-8')
                    latestString = latestString.split("^^^^")
                    responseString=""
                    if len(latestString) >= 20:
                        itemsAppended=0
                        for x in reversed(latestString):
                            if itemsAppended==20:
                                break
                            x = x.split(":")
                            #if()
                            responseString+= x[0] + "---->" +x[1] +"\n"
                            itemsAppended+=1
                    elif len(latestString) < 20:
                        for x in reversed(latestString):
                            x = x.split(":")
                            #if()
                            responseString+= x[0] + "---->" +x[1] +"\n"

                '''


            ## Query types sent to be handled by frontEndServer

            if req['query'] =='searchUserAndExpand':
                print(req)
                print('Query received successfully')

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



                #TODO Complete this
                # raise NotImplementedError;


                if 'post_nos' in req.keys():
                    for i in req['post_nos']:
                        user_postno.append(req['name']+'#'+str(i))
                else:

                    requested_numposts = int(req['num'])

                    while tweets > 0 and requested_numposts > 0:
                        del_list = []
                        deleted = str(req['name'])+'#-'+ str((int((tweets-1)/100)+1))
                        del_postnos = memc.get(deleted)
                        if not del_postnos:
                            qu = "SELECT userhash FROM status WHERE userhash='" + deleted +"'"
                            conn.query(qu)
                            rows = conn.store_result()
                            rows = rows.fetch_row(how=1,maxrows=0)
                            if len(rows)==0:
                                ## No rows for that range is deleted
                                del_postnos=""
                                pass
                            else:
                                del_postnos = rows[0]
                        del_postnos = del_postnos.split(',')
                        for i in del_postnos:
                            try:
                                del_list.append(int(i))
                            except:
                                continue


                        while tweets >= 1 and requested_numposts >= 1:
                            user_postno.append(req['name']+'#'+str(tweets))
                            tweets -=1
                            requested_numposts -=1
                            if tweets%100 ==0:
                                break
                """    requested_numposts = int(req['num'])
                    while tweets >= 1 and requested_numposts >= 1:
                        user_postno.append(req['name']+'#'+str(tweets))
                        tweets -=1
                        requested_numposts -=1
                """



                ini = int(req['ini'])
                nodes = req['nodes']
                N = len(nodes)

                tasks = {}
                for i in range(0,N):
                    if i == ini:
                        continue
                    # nodes[hash(req_value)%N] = i
                    if i in tasks.keys():
                        tasks[i].append()

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






        except:
            print(str(e))
            traceback.print_exc()
        finally:
            if 'memc' in locals():
                memc.close()
            c.close()
            exit(0)

    else:
        c.close()

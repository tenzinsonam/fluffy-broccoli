import json
import datetime
import sys
import socket

SERVER_IP = '127.0.0.1'
PORT = 24001

debug = True

def debug_print(debug_str):
    if(debug):
        print(debug_str)

def setMessage(inp):
    str1 = str(len(inp))
    while(len(str1)<10):
        str1 = "0" + str1
    return str1.encode('utf-8') + inp

def getMessage(s):
    rawn = s.recv(10).decode('UTF-8')
    if(len(rawn)==0):
        return ""
    print(rawn)
    return s.recv(int(rawn)).decode('UTF-8')

s = socket.socket()
s.connect((SERVER_IP, PORT))

inp = ""

while True:
    print('Are you already a user - Y/N ?')
    inp = input()
    if inp.lower() not in ['y','n']:
        print('Invalid Input...')
        continue
    break;


# UserExists? - 0 => useralready exists
user =""
print('Enter your username')
while True:
    username = input()
    if inp.lower() == 'n':
        data = {'query':'UserExists?','value':username,'createUserIfNotExists':True}
        s.sendall(setMessage((json.dumps(data)).encode('UTF-8')))
        jrec = json.loads(getMessage(s))
        if jrec['code']==0:
            print('Pick a new user, given user already exists')
            continue
        elif jrec['code']==1:
            user = username
            break
        else:
            print('Something went wrong')
    elif inp.lower() == 'y':
        data = {'query':'UserExists?','value':username,'createUserIfNotExists':False}
        s.sendall(setMessage((json.dumps(data)).encode('UTF-8')))
        jrec = json.loads(getMessage(s))
        if jrec['code']==0:
            user = username
            break
        elif jrec['code']==1:
            print('Your account does not exists')
            raise SystemExit
        else:
            print('Something went wrong')
    else:
        print('Something went wrong')
        print(jrec)

print("Welcome !!!\n You logged in sucessfully")
time2 = datetime.datetime.now()
'''
if inp is 'Y' or inp is 'y':
    print('good')
    #handle for this case
else:
    data = {'query':'UserExists?','value':username}
    json_string = json.dumps(data)
    print(json_string)
    s.sendall(setMessage((json_string).encode('UTF-8')))

    #time1 = datetime.datetime.now()

    rec = getMessage(s)
    jrec = json.loads(rec)
    print(jrec)
    if jrec['code']==0:
        print(jrec['response'])

    print(rec)
    time2 = datetime.datetime.now()
'''

#print('''
#    Command Rules
#    search <username> <#ofPosts>
#    update <enter>
#        <Your Post>
#    ''')
print('Enter query type')
que = input()
if que == "search":
    print('username to search')
    nm = input()
    print('number of posts')
    postn = input()
    data = {'query':'searchUser', 'name':nm, 'num':postn}
    json_string = json.dumps(data)
    #print(json_string)
    s.sendall(setMessage((json_string).encode('UTF-8')))
    rec = getMessage(s)
    print(rec)

elif que == "update":
    print("write your post")
    pst = input()
    data = {'query':'updateUserinfo','name':username,'value':pst}
    json_string = json.dumps(data)
    #print(json_string)
    s.sendall(setMessage((json_string).encode('UTF-8')))
    rec = getMessage(s)
    jrec = json.loads(rec)
    if jrec['code']==1:
        print(jrec['response'])
    #print(rec)
s.close()

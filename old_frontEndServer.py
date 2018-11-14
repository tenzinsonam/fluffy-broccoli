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

# nodes must contain different ips
nodes = [{'ip':'127.0.0.1','port':12345}]
N = len(nodes)

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
#memc = base.Client(('127.0.0.1',11211));
'''
memc = HashClient(
    ('localhost', 11211),
    ('localhost', 11212)
])
'''
#conn = _mysql.connect (host = "localhost",
#                        user = "root",
#                        passwd = "tenzin",
#                        db = "cs632")

s = socket.socket()
s.bind(('', int(sys.argv[1])))
s.listen(500)

# Request to be processed
while True:

    c, addr = s.accept()
    if not os.fork():
        s.close()
        try:
            req = getMessage(c)
            print(req)
            req = json.loads(req)

            if req['query'] in ['UserExists?']:
                req_value = req['value']+'#0'
                handlingnode = nodes[hash(req_value)%N]
                s_handlingnode = socket.socket()
                s_handlingnode.connect((handlingnode['ip'],handlingnode['port']))
                s_handlingnode.sendall(setMessage((json.dumps(req)).encode('UTF-8')))
                message = getMessage(s_handlingnode)
                s_handlingnode.close()
                c.sendall(setMessage(message).encode('UTF-8'))
            elif req['query'] in ['updateUserinfo']:
                req_value = req['name']+'#0'
                intended_node_index = hash(req_value)%N
                handlingnode = nodes[intended_node_index]
                s_handlingnode = socket.socket()
                s_handlingnode.connect((handlingnode['ip'],handlingnode['port']))
                newquery = {'query':'updateUserinfoAndExpand','name':req['name'],'nodes':nodes,'ini':intended_node_index,'port':s_common.getsockname()[1],'value':req['value']}
                s_handlingnode.sendall(setMessage(json.dumps(newquery).encode('UTF-8')))
                s_handlingnode.close()
                c.sendall(setMessage(json.dumps({'code':1,'response':'Status will be updated'})).encode('UTF-8'))
                # # TODO:
                # Fetch status, for the open a port etc etc

            elif req['query'] in ['searchUser']:
                req_value = req['name']+'#0'
                handlingnode = nodes[intended_node_index]
                s_handlingnode = socket.socket()
                s_handlingnode.connect((handlingnode['ip'],handlingnode['port']))
                intended_node_index = hash(req_value)%N

                s_common = socket.socket()
                s_common.bind(('',0))
                s_common.listen(N)

                newquery = {'query':'searchUserAndExpand','name':req['name'],'nodes':nodes,'ini':intended_node_index,'port':s_common.getsockname()[1],'num':req['num']}
                s_handlingnode.sendall(setMessage(json.dumps(newquery).encode('UTF-8')))
                s_handlingnode.close()


                count = 0 #for number of messages
                maxcount = -1
                concatenatedMessage = ""
                while True:
                    c_common, addr_common = s_common.accept()
                    response = getMessage(c_common)
                    if 'queryResponse' in response.keys() and response['queryResponse']=='searchUserAndExpand':
                       maxcount = response['totalNumResponses']
                    concatenatedMessage += response['message']
                    count+=['numResponse']
                    if maxcount==count:
                        break;
                s_common.close()
                c.sendall(setMessage(concatenatedMessage))
        except Exception as e:
            print(str(e))
            traceback.print_exc()
        finally:
            c.close()
            exit(0)

    else:
        c.close()

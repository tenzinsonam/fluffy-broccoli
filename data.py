import sys
import _mysql
from pymemcache.client import base
from pymemcache.client.hash import HashClient
import socket

PORT = 24000
#memc = base.Client(('127.0.0.1',11211));
memc = HashClient([
    ('localhost', 11211),
    ('localhost', 11212)
])

conn = _mysql.connect (host = "localhost",
                        user = "tenzin",
                        passwd = "tenzin",
                        db = "cs632")




popularfilms = memc.get('top5films')
if not popularfilms:
    #cursor = conn.cursor()
    qu = 'SELECT * FROM filmorder'
    conn.query(qu)
    rows = conn.store_result()
    rows = rows.fetch_row(how=1,maxrows=0)
    #for x in ro
    #print(rows)
    memc.set('top5films',rows,60)
    print("Updated memcached with MySQL data")
else:
    print("Loaded data from memcached")
    print(popularfilms)


cunt = memc.get('bunt')
if not cunt:
    asdf = {'heck':'asdf', 'sdaf':'reasf'}
    memc.set('bunt', asdf, 60)
else:
    print(cunt.decode('UTF-8'))

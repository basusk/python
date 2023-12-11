#!/usr/bin/python
###### Author: Saikat Basu, Senior Solution Architect, Kyligence ##########
###### Revision 1 : Single threaded; Date: 21st October 2020  #############
###### Revision 2 : Created PerfTest as callable function; Date: 22nd October 2020 ######### 
###### Revision 3 : Multithreaded; Parameterized and configurable Thread count and loop count; Date: 23rd October 2020   ############# 
###### Revision 4 : Separated all configuration parameters and SQL queries in a different cfg.ini file; Date: 13th November 2020   ############# 

import logging
logging.basicConfig(level=logging.DEBUG)

from sqlalchemy import create_engine, inspect

import threading
import time
import ConfigParser

config = ConfigParser.ConfigParser()
config.readfp(open(r'./cfg_multithread_kc.ini'))

username = config.get('connection', 'username')
password = config.get('connection', 'password')
url = config.get('connection', 'url')
project = config.get('connection', 'project')

print ("User: " + username + "; Password: " + password + "; URL: " + url + "; Project:" + project)

threadCount_str = config.get('threading', 'threadCount')
loopCount_str = config.get('threading', 'loopCount')

print("Thread count: " + threadCount_str + "; Loop count: " + loopCount_str)

threadCount = int(threadCount_str)
loopCount = int(loopCount_str)

sql1 = config.get('sqls', 'sql1')
sql2 = config.get('sqls', 'sql2')
sql3 = config.get('sqls', 'sql3')
sql4 = config.get('sqls', 'sql4')
sql5 = config.get('sqls', 'sql5')
sql6 = config.get('sqls', 'sql6')
sql7 = config.get('sqls', 'sql7')
sql8 = config.get('sqls', 'sql8')
sql9 = config.get('sqls', 'sql9')
sql10 = config.get('sqls', 'sql10')
sql11 = config.get('sqls', 'sql11')
sql12 = config.get('sqls', 'sql12')
sql13 = config.get('sqls', 'sql13')
sql14 = config.get('sqls', 'sql14')
sql15 = config.get('sqls', 'sql15')
sql16 = config.get('sqls', 'sql16')
sql17 = config.get('sqls', 'sql17')
sql18 = config.get('sqls', 'sql18')
sql19 = config.get('sqls', 'sql19')
sql20 = config.get('sqls', 'sql20')
sql21 = config.get('sqls', 'sql21')

class myThread (threading.Thread) :
   def __init__(self, threadID, name):
      threading.Thread.__init__(self)
      self.threadID = threadID
      self.name = name
   
   def run(self) :
      print "Starting " + self.name
      print "CALLING PERFTEST"
      perfTest(username, password, url, project)
      print "Exiting " + self.name

def perfTest(username, password, cluster_url, project):
    print "ENTERED PERFTEST"
    
    kylin = create_engine("kylin://" + username + ":" + password + "@" + cluster_url + "/" + project + "?version=v4")
    

    queries = []
    
   
    for n in range (loopCount) :
        queries = [sql1, sql2, sql3, sql4, sql5, sql6, sql7, sql8, sql9, sql10, sql11, sql12, sql13, sql14, sql15, sql16, sql17, sql18, sql19, sql20, sql21]
        for x in queries :
            rp = kylin.execute(x, limit=10, offset=0)
#      	    print(rp.fetchall)

    
threads = []

for i in range (threadCount) :
    threadName = 'thr_'+str(i)
    print "THREADNAME : "+threadName
    thread = myThread(i, 'thr_'+str(i))
    threads.append(thread)

print threads

for t in threads :
    t.start()	


print "Exiting Main Thread"

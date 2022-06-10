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
config.readfp(open(r'cfg_multithread.ini'))

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
        queries = [sql1, sql2, sql3, sql4]
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

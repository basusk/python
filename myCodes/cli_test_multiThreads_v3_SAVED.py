#!/usr/bin/python
###### Author: Saikat Basu, Senior Solution Architect, Kyligence ##########
###### Revision 1 : Single threaded; Date: 21st October 2020  #############
###### Revision 2 : Created PerfTest as callable function; Date: 22nd October 2020 ######### 
###### Revision 3 : Multithreaded; Parameterized and configurable Thread count and loop count; Date: 23rd October 2020   ############# 

import logging

import threading
import time

threadCount = 5
loopCount = 1000

from sqlalchemy import create_engine, inspect

logging.basicConfig(level=logging.DEBUG)

class myThread (threading.Thread) :
   def __init__(self, threadID, name):
      threading.Thread.__init__(self)
      self.threadID = threadID
      self.name = name
   
   def run(self) :
      print "Starting " + self.name
      print "CALLING PERFTEST"
      perfTest("SAIKAT", "KYLIN!123", "localhost:7070", "myProj1")
      print "Exiting " + self.name

def perfTest(username, password, cluster_url, project):
    print "ENTERED PERFTEST"
    
    kylin = create_engine("kylin://" + username + ":" + password + "@" + cluster_url + "/" + project + "?version=v4")
    
    sql1 = """
    select
        "CUSTOMER"."C_MKTSEGMENT" as "c0",
        "CUSTOMER"."C_REGION" as "c1",
        "CUSTOMER"."C_NATION" as "c2", 
        sum("LINEORDER"."LO_QUANTITY") as "m0",
        sum("LINEORDER"."LO_REVENUE") as "m1",
        sum("LINEORDER"."LO_SUPPLYCOST") as "m2"
    from
        "SSB"."LINEORDER" as "LINEORDER" left join "SSB"."CUSTOMER" as "CUSTOMER" on "LINEORDER"."LO_CUSTKEY" = "CUSTOMER"."C_CUSTKEY"
    group by
        "CUSTOMER"."C_MKTSEGMENT",
        "CUSTOMER"."C_REGION",
        "CUSTOMER"."C_NATION"
    order by
        "CUSTOMER"."C_MKTSEGMENT" ASC,
        "CUSTOMER"."C_REGION" ASC,
        "CUSTOMER"."C_NATION" ASC
    LIMIT 5
    """  # noqa

    sql2 = """
    SELECT 
    DATES.D_YEAR, DATES.D_DATE,count(1)
    FROM 
    "SSB"."LINEORDER" as "LINEORDER" 
    LEFT JOIN "SSB"."CUSTOMER" as "CUSTOMER" ON "LINEORDER"."LO_CUSTKEY"="CUSTOMER"."C_CUSTKEY"
    LEFT JOIN "SSB"."DATES" as "DATES" ON "LINEORDER"."LO_ORDERDATE"="DATES"."D_DATEKEY"
    LEFT JOIN "SSB"."PART" as "PART" ON "LINEORDER"."LO_PARTKEY"="PART"."P_PARTKEY"
    LEFT JOIN "SSB"."SUPPLIER" as "SUPPLIER" ON "LINEORDER"."LO_SUPPKEY"="SUPPLIER"."S_SUPPKEY"
    group by CUSTOMER.C_NATION, CUSTOMER.C_CITY, CUSTOMER.C_REGION, DATES.D_YEAR, DATES.D_DATE, PART.P_BRAND
    limit 5
    """

    sql3 = """
    select
        "CUSTOMER"."C_MKTSEGMENT" as "c0",
        "CUSTOMER"."C_REGION" as "c1",
        "CUSTOMER"."C_NATION" as "c2",
        sum("LINEORDER"."LO_QUANTITY") as "m0",
        sum("LINEORDER"."LO_REVENUE") as "m1",
        sum("LINEORDER"."LO_SUPPLYCOST") as "m2"
    from
        "SSB"."LINEORDER" as "LINEORDER" left join "SSB"."CUSTOMER" as "CUSTOMER" on "LINEORDER"."LO_CUSTKEY" = "CUSTOMER"."C_CUSTKEY"
    group by
        "CUSTOMER"."C_MKTSEGMENT",
        "CUSTOMER"."C_REGION",
        "CUSTOMER"."C_NATION"
    order by
        "CUSTOMER"."C_MKTSEGMENT" ASC,
        "CUSTOMER"."C_REGION" ASC,
        "CUSTOMER"."C_NATION" ASC
    limit 5
    """
    queries = []
    
   
    for n in range (loopCount) :
        queries = [sql1, sql2, sql3]
        for x in queries :
            rp = kylin.execute(x, limit=10, offset=0)
            print(rp.fetchall)

    
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

#!/usr/bin/python

import logging

import threading
import time
exitFlag = 0
from sqlalchemy import create_engine, inspect

logging.basicConfig(level=logging.DEBUG)

class myThread (threading.Thread) :
   def __init__(self, threadID, name, counter):
      threading.Thread.__init__(self)
      self.threadID = threadID
      self.name = name
      self.counter = counter
   
   def run(self) :
      print "Starting " + self.name
      #print_time(self.name, 5, self.counter)
      print "CALLING PERFTEST"
      perfTest("SAIKAT", "KYLIN!123", "localhost:7070", "myProj1")
      print "Exiting " + self.name


def print_time(threadName, counter, delay) :
   while counter:
      if exitFlag:
         threadName.exit()
      time.sleep(delay)
      print "%s: %s" % (threadName, time.ctime(time.time()))
      counter -= 1

def perfTest(username, password, cluster_url, project):
#def perfTest ()
    print "ENTERED PERFTEST"
    kylin = create_engine("kylin://" + username + ":" + password + "@" + cluster_url + "/" + project + "?version=v4")
    #kylin = create_engine('kylin://SAIKAT:KYLIN!123@localhost:7070/myProj1?version=v4')
    sql1 = """
    SELECT "EXPENSE_TYPE" AS "EXPENSE_TYPE",
       count("EXTERNAL_REPORT_ID") AS "COUNT(EXTERNAL_REPORT_ID)"
    FROM "FACTS_INSIGHTS_PRO_AUDIT_ACTIVITY"
    WHERE "SUBMISSION_DATE" >= CAST('2019-07-02' AS DATE)
    AND "SUBMISSION_DATE" < CAST('2019-11-15' AS DATE)
    AND "EXCLUDE_REASON" = 'FPOS'
    AND "CUSTOMER_ID" = 2744
    AND "ORGANIZATION_NAME" IN ('NO')
    AND "AUDITOR_ID" IN ('kjacama@salesforce.com')
    GROUP BY "EXPENSE_TYPE"
    ORDER BY "COUNT(EXTERNAL_REPORT_ID)" DESC
    LIMIT 10
    """  # noqa

    sql2 = """
    SELECT "ACTION" AS "ACTION",
       "AUDITOR_ID" AS "AUDITOR_ID",
       count("ACTION") AS "COUNT(ACTION)"
    FROM "FACTS_INSIGHTS_PRO_AUDIT_ACTIVITY"
    WHERE "SUBMISSION_DATE" >= CAST('2019-07-02' AS DATE)
    AND "SUBMISSION_DATE" < CAST('2019-11-15' AS DATE)
    AND "CUSTOMER_ID" = 2744
    AND "ORGANIZATION_NAME" IN ('NO')
    AND "AUDITOR_ID" IN ('kjacama@salesforce.com')
    GROUP BY "ACTION",
         "AUDITOR_ID"
    ORDER BY "COUNT(ACTION)" DESC
    LIMIT 10
    """

    sql3 = """
    SELECT "AUDITOR_ID" AS "AUDITOR_ID",
       count("LINE_ID") AS "Number of Lines"
    FROM "FACTS_INSIGHTS_PRO_AUDIT_ACTIVITY"
    WHERE "SUBMISSION_DATE" >= CAST('2019-03-21' AS DATE)
    AND "SUBMISSION_DATE" < CAST('2020-03-20' AS DATE)
    AND "CUSTOMER_ID" = 2744
    AND "AUDITOR_ID" IN ('pclemente@salesforce.com')
    GROUP BY "AUDITOR_ID"
    ORDER BY "Number of Lines" DESC
    LIMIT 10
    """
    sql4 = """
    SELECT CAST(FLOOR(CAST("SUBMISSION_DATE" AS TIMESTAMP) TO MONTH) AS DATE) AS "__timestamp",
       AVG("RISK_TO_ACTION_TIME") AS "Average Risk to Action Time"
    FROM "FACTS_INSIGHTS_PRO_AUDIT_ACTIVITY"
    WHERE "SUBMISSION_DATE" >= CAST('2019-03-21' AS DATE)
    AND "SUBMISSION_DATE" < CAST('2020-03-20' AS DATE)
    AND "CUSTOMER_ID" = 2744
    AND "ORGANIZATION_NAME" IN ('JP')
    AND "AUDITOR_ID" IN ('pclemente@salesforce.com')
    GROUP BY CAST(FLOOR(CAST("SUBMISSION_DATE" AS TIMESTAMP) TO MONTH) AS DATE)
    ORDER BY "Average Risk to Action Time" DESC
    LIMIT 10

    sql5 = """
    SELECT "AUDITOR_ID" AS "AUDITOR_ID",
       count("LINE_ID") AS "Number of Lines"
    FROM "FACTS_INSIGHTS_PRO_AUDIT_ACTIVITY"
    WHERE "SUBMISSION_DATE" >= CAST('2019-03-21' AS DATE)
    AND "SUBMISSION_DATE" < CAST('2020-03-20' AS DATE)
    AND "CUSTOMER_ID" = 2744
    AND "AUDITOR_ID" IN ('pclemente@salesforce.com')
    GROUP BY "AUDITOR_ID"
    ORDER BY "Number of Lines" DESC
    LIMIT 10
    """

    sql6 = """
    SELECT CAST(FLOOR(CAST("SUBMISSION_DATE" AS TIMESTAMP) TO MONTH) AS DATE) AS "__timestamp",
       AVG("RISK_TO_ACTION_TIME") AS "Average Risk to Action Time"
    FROM "FACTS_INSIGHTS_PRO_AUDIT_ACTIVITY"
    WHERE "SUBMISSION_DATE" >= CAST('2019-05-12' AS DATE)
    AND "SUBMISSION_DATE" < CAST('2020-01-04' AS DATE)
    AND "ORGANIZATION_NAME" IN ('DE')
    AND "AUDITOR_ID" IN ('sbaldovino@salesforce.com')
    GROUP BY CAST(FLOOR(CAST("SUBMISSION_DATE" AS TIMESTAMP) TO MONTH) AS DATE)
    ORDER BY "Average Risk to Action Time" DESC
    LIMIT 10
    """

    sql7 = """
    SELECT "EXPENSE_TYPE" AS "EXPENSE_TYPE",
       "EXCLUDE_REASON" AS "EXCLUDE_REASON",
       count("EXTERNAL_REPORT_ID") AS "COUNT(EXTERNAL_REPORT_ID)"
    FROM "FACTS_INSIGHTS_PRO_AUDIT_ACTIVITY"
    WHERE "SUBMISSION_DATE" >= CAST('2020-02-19' AS DATE)
    AND "SUBMISSION_DATE" < CAST('2020-03-20' AS DATE)
    AND "ACTION" = 'APPROVE'
    AND "CUSTOMER_ID" = 2744
    GROUP BY "EXPENSE_TYPE",
         "EXCLUDE_REASON"
    ORDER BY "COUNT(EXTERNAL_REPORT_ID)" DESC
    LIMIT 10
    """

    sql8 = """
    SELECT "AUDITOR_ID" AS "AUDITOR_ID",
       count("LINE_ID") AS "Number of Lines"
    FROM "FACTS_INSIGHTS_PRO_AUDIT_ACTIVITY"
    WHERE "SUBMISSION_DATE" >= CAST('2019-03-21' AS DATE)
    AND "SUBMISSION_DATE" < CAST('2020-03-20' AS DATE)
    AND "CUSTOMER_ID" = 2744
    AND "AUDITOR_ID" IN ('pclemente@salesforce.com')
    GROUP BY "AUDITOR_ID"
    ORDER BY "Number of Lines" DESC
    LIMIT 10
    """

    sql9 = """
    SELECT "AUDITOR_ID" AS "AUDITOR_ID",
       count("LINE_ID") AS "Number of Lines"
    FROM "FACTS_INSIGHTS_PRO_AUDIT_ACTIVITY"
    WHERE "SUBMISSION_DATE" >= CAST('2020-02-19' AS DATE)
    AND "SUBMISSION_DATE" < CAST('2020-03-20' AS DATE)
    AND "CUSTOMER_ID" = 2744
    AND "ORGANIZATION_NAME" IN ('NO')
    AND "AUDITOR_ID" IN ('kjacama@salesforce.com')
    GROUP BY "AUDITOR_ID"
    ORDER BY "Number of Lines" DESC
    LIMIT 10
    """
    
    sql10 = """
    SELECT CAST(FLOOR(CAST("SUBMISSION_DATE" AS TIMESTAMP) TO MONTH) AS DATE) AS "__timestamp",
       AVG("RISK_TO_ACTION_TIME") AS "Average Risk to Action Time"
    FROM "FACTS_INSIGHTS_PRO_AUDIT_ACTIVITY"
    WHERE "SUBMISSION_DATE" >= CAST('2019-03-21' AS DATE)
    AND "SUBMISSION_DATE" < CAST('2020-03-20' AS DATE)
    AND "CUSTOMER_ID" = 2744
    AND "AUDITOR_ID" IN ('fmamaril@salesforce.com')
    GROUP BY CAST(FLOOR(CAST("SUBMISSION_DATE" AS TIMESTAMP) TO MONTH) AS DATE)
    ORDER BY "Average Risk to Action Time" DESC
    LIMIT 10
    """

    for n in range (1) :
        queries = [sql1, sql2, sql3]
        for x in queries :
            rp = kylin.execute(x, limit=10, offset=0)
            print(rp.fetchall)
        


#kylin = create_engine('kylin://SAIKAT:KYLIN!123@localhost:7070/myProj1?version=v4')
#print(kylin.table_names())

#insp = inspect(kylin)
#print(insp.get_schema_names())
#print(insp.get_columns('CUSTOMER', 'SSB'))

# Create new threads
thread1 = myThread(1, "Thread-1", 1)
thread2 = myThread(2, "Thread-2", 2)

# Start new Threads
thread1.start()
thread2.start()

print "Exiting Main Thread"

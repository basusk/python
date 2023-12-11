#!/usr/bin/python
###### Author: Saikat Basu, Senior Solution Architect, Kyligence ##########
###### Revision 1 : Added codes for accelerate SQL API to identify biggest(superset query) from a list of queries; Date: Mar 20th 2023 ############# 
###### Revision 1 : And create a base model for that and recommend indexes for other queries on that base model; Date: Mar 22nd 2023 ############# 
###### Revision 1 : Modified program to accept fractional number of hours as query download window; Date: April 6th 2023 ############# 

import requests
import json
import ConfigParser

import sys
import time

config = ConfigParser.ConfigParser()
config.readfp(open(r'cfg_sql_accelerate_v1.ini'))

host = config.get('connection', 'host')
port = config.get('connection', 'port')
cfg_project = config.get('connection', 'project')

qry_hist_api = config.get('api', 'qry_hist_api')
sql_accelerate_api = config.get('api', 'sql_accelerate_api')

numberOfHours = config.get('api', 'numHours')

accept = config.get('header', 'Accept')
accept_language = config.get('header', 'Accept-Language')
content_type = config.get('header', 'Content-Type')
auth = config.get('header', 'Authorization')

kylin_headers = {'Accept': str(accept), 'Accept-Language': str(accept_language), 'Authorization': str(auth), 'Content-Type': str(content_type)}


def pushDownQueries():

	current_time_ms = int( time.time() * 1000 )
	#print str(current_time_ms)
	
	current_time = float(current_time_ms)

        hours = float(numberOfHours)

	print ("Downloading Query History for " + str(hours) + " hours\n")

	milliseconds = (hours*3600000.0)
	#print ("milliseconds: " +  str(milliseconds))

	one_day_before_ms = int(current_time - milliseconds)
	#print ("Start Time: " + str(one_day_before_ms))

	QRY_HIST_URL = host + ":" + port + "/" + qry_hist_api + "?project=" + cfg_project + "&page_size=1000&limit=1000&start_time_from=" + str(one_day_before_ms) + "&start_time_to=" + str(current_time_ms) 

	print ("Sending Query History URL ==> " + QRY_HIST_URL + "\n")

	qry_hist_response = requests.get(QRY_HIST_URL, headers = kylin_headers)
	print ("Printing Qry History Response: " + (str(qry_hist_response)))

	print "Response eeceived for query history \n"
	print "################################# \n"
	print "Number of queries in history: " + str(qry_hist_response.json()['data']['size'])
	print "################################# \n"
	queries = []
	queries = qry_hist_response.json()['data']['query_histories']

	qryCount = len(queries)

	print ("QryCount: " + str(qryCount) + "\n")
	if qry_hist_response.status_code != 200 :
        	print ('Error : ' + str(qry_hist_response.status_code))
       		sys.exit()
	else :
        	print ('API Response Status Code : ' + str(qry_hist_response.status_code) + ' Status : SUCCESS')

	queries = []
	queries = qry_hist_response.json()['data']['query_histories']

	sqls = []
	sqlCounter = 0

	for query in queries:
        #	print (str(query['query_status']) + "," + str(query['index_hit']) + "," + str(query['engine_type']))

		#if ((str(query['query_status']) == 'SUCCEEDED') and (str(query['index_hit']) == 'False')  and (str(query['engine_type']) == 'OBJECT STORAGE')):
		if ((str(query['query_status']) == 'SUCCEEDED') and (str(query['index_hit']) == 'False')  and (str(query['engine_type']) == 'HIVE')):
			sqls.append (str(query['sql_pattern']))
			sqlCounter += 1

	print ("################################# \n")
	print ("Out of total: " + str(qryCount) + " queries, # of queries answered by pushdown: " + str(sqlCounter))
	print ("################################# \n")

	#print str(len(sqls))
	return sqls

pdQueries = []
pdQueries = pushDownQueries()

if (len(pdQueries) == 0):
	print ("There is no pushdown queries.....Existing...")
	sys.exit()

print ("Number of Push Down Queries : " + str(len(pdQueries)))


def accelerate_sql(list_of_push_down_queries):
        sql_accelerate_url = host + ":" + port + "/" + sql_accelerate_api
        print (sql_accelerate_url + "\n")
        url_body = {
                        "project":cfg_project,
                        "sqls":list_of_push_down_queries
                   }
        print (str(url_body) + "\n")

        print ("Sending SQL Accelerate URL ==> " + sql_accelerate_url + "\n")
        sql_accelerate_response = requests.post(sql_accelerate_url, json = url_body, headers = kylin_headers)

        print (sql_accelerate_response.content)

	return True

flag = False
flag = accelerate_sql(pdQueries)

print ("Check if models created")



#!/usr/bin/python
###### Author: Saikat Basu, Senior Solution Architect, Kyligence ##########
###### Revision 1 : This script will detect pushdown queries from query history    #############
###### Revision 1 : Then will send create model API call with list of push down queries ######### 
###### Revision 1 : Date: 14th November 2022   ############# 
###### Revision 2 : Added no.of Hrs as configurable parameter; Date: 20th December 2022   ############# 


import requests
import json
import ConfigParser

import time

config = ConfigParser.ConfigParser()
config.readfp(open(r'cfg_model_create.ini'))

host = config.get('connection', 'host')
port = config.get('connection', 'port')
project = config.get('connection', 'project')

qry_hist_api = config.get('api', 'qry_hist_api')
model_create_api = config.get('api', 'model_create_api')
#model_validate_api = config.get('api', 'model_validate_api')
numberOfHours = config.get('api', 'numHours')

accept = config.get('header', 'Accept')
accept_language = config.get('header', 'Accept-Language')
content_type = config.get('header', 'Content-Type')
auth = config.get('header', 'Authorization')

kylin_headers = {'Accept': str(accept), 'Accept-Language': str(accept_language), 'Authorization': str(auth), 'Content-Type': str(content_type)}


def pushDownQueries():

	current_time_ms = int( time.time() * 1000 )
	print str(current_time_ms)

        hours = int(numberOfHours)
	print ("# of hours: " + str(hours))

	milliseconds = (hours*3600000)
	print ("milliseconds: " +  str(milliseconds))

	one_day_before_ms = current_time_ms - milliseconds
	print (str(one_day_before_ms))

	QRY_HIST_URL = host + ":" + port + "/" + qry_hist_api + "?project=" + project + "&page_size=1000&limit=1000&start_time_from=" + str(one_day_before_ms) + "&start_time_to=" + str(current_time_ms) 

	print ("Sending Query History URL ==> " + QRY_HIST_URL + "\n")

	qry_hist_response = requests.get(QRY_HIST_URL, headers = kylin_headers)
	#print (str(qry_hist_response))

	#print "Response received for query history \n"
	#print "################################# \n"
	#print "Number of queries in history: " + str(qry_hist_response.json()['data']['size'])
	#print "################################# \n"
	queries = []
	queries = qry_hist_response.json()['data']['query_histories']

	qryCount = len(queries)

	print ("QryCount: " + str(qryCount))
	if qry_hist_response.status_code != 200 :
        	print ('Error : ' + str(qry_hist_response.status_code))
       		exit()
	else :
        	print ('API Response Status Code : ' + str(qry_hist_response.status_code) + ' Status : SUCCESS')

	queries = []
	queries = qry_hist_response.json()['data']['query_histories']

	sqls = []
	sqlCounter = 0

	for query in queries:
        	#print (str(query['query_status']) + "," + str(query['index_hit']) + "," + str(query['engine_type']))

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

print ("Number of Push Down Queries : " + str(len(pdQueries)))

def createModels(list_of_push_down_queries):
	model_create_url = host + ":" + port + "/" + model_create_api
        #print (model_create_url + "\n")
	url_body = {
			"project":project,
			"sqls":list_of_push_down_queries
		   }
	#print (str(url_body) + "\n")

	print ("Sending Model Create URL ==> " + model_create_url + "\n")
	model_create_response = requests.post(model_create_url, json = url_body, headers = kylin_headers)
        #print str(model_create_response.json())
	response_code = model_create_response.json()['code']
	print ("API Response Received, Parsing Response")
	if (str(response_code) != '000'):
		print ("Model create API fails")
		return False
	elif (str(response_code) == '000'):
		print ("Model create API succeeds")
		return True

model_create = createModels(pdQueries)

if (str(model_create) == 'True'):
	print ("Model Created Successfully")

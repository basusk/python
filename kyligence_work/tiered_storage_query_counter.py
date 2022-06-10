#!/usr/bin/python
###### Author: Saikat Basu, Senior Solution Architect, Kyligence ##########
###### Revision 1 : Counting total number of queries answered by Tiered Storage; Date: 15th April 2022 ###########
###### Revision 2 : Running the program in a loop; Date: 15th April 2022 ###########
###### Revision 3 : Changed logic to detect query answered by TS via secondStorage; Date: 19 April 2022 ##########

import requests
import json

import ConfigParser

import time

config = ConfigParser.ConfigParser()
config.readfp(open(r'qryhstr_test_cfg.ini'))

host = config.get('connection', 'host')
port = config.get('connection', 'port')
workspace = config.get('connection', 'workspace')
project = config.get('connection', 'project')

user_api = config.get('api', 'user_api')
qry_hist_api = config.get('api', 'qry_hist_api')

accept = config.get('header', 'Accept')
accept_language = config.get('header', 'Accept-Language')
content_type = config.get('header', 'Content-Type')
auth = config.get('header', 'Authorization')

kylin_headers = {'Accept': str(accept), 'Accept-Language': str(accept_language), 'Authorization': str(auth), 'Content-Type': str(content_type)}

QRY_HIST_URL = host + ":" + port + "/" + "workspaces/" + workspace + "/" + qry_hist_api + "?project=" + project + "&page_size=10000"

while(True):

#	print "Sending URL ==> " + QRY_HIST_URL + "\n"

	qry_hist_response = requests.get(QRY_HIST_URL, headers = kylin_headers)

#	print "Response received for query history \n"
#	print "################################# \n"
#	print "Number of queries in history: " + str(qry_hist_response.json()['data']['size'])
#	print "################################# \n"
	queries = []
	queries = qry_hist_response.json()['data']['query_histories']

	qryCount = len(queries)

##	print "QryCount: " + str(qryCount)
#	print ""
	if qry_hist_response.status_code != 200 :
        	print ('Error : ' + str(qry_hist_response.status_code))

       		exit()
#	else :
#        	print ('Status Code : ' + str(qry_hist_response.status_code) + ' Status : SUCCESS')

	tsCounter = 0

	for query in queries:
		id = query['query_id']
	#	print "Query Id: " + str(id)
		realization_metrics = []
		realization_metrics = query['query_history_info']['realization_metrics']
		for realization in realization_metrics:
#			print "Query answered by: " + str(realization['indexType'])
#			print "Query answered by: " + str(realization['secondStorage'])
#			print "Query answered by index id: " + str(realization['layoutId'])
#			if (str(realization['layoutId'])) == '20000000001':
			if (str(realization['secondStorage'])) == 'True':
				tsCounter = tsCounter + 1

	print "################################# \n"
	print "Out of total: " + str(qryCount) + " queries, # of queries answered by Tiered Storage: " + str(tsCounter)
	print "################################# \n"
	time.sleep(10)

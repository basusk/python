#!/usr/bin/python
###### Author: Saikat Basu, Senior Solution Architect, Kyligence ##########
###### Revision 1 : Query load calculator; Date: 13th June  2022 ###########
###### Revision 2 : Changed to make API call with time duration 18th July 2022 ##########

import requests
import json

import ConfigParser

import time

config = ConfigParser.ConfigParser()
config.readfp(open(r'simple_qps_counter.ini'))

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

def funcCounter(start_time,end_time):
	qryCounter = 0
	aggQryCounter = 0
	detailQryCounter = 0

	QRY_HIST_URL = host + ":" + port + "/" + "workspaces/" + workspace + "/" + qry_hist_api + "?project=" + project + "&limit=100&offset=0&start_time_from=" + str(start_time) + "&start_time_to=" + str(end_time)

	qry_hist_response = requests.get(QRY_HIST_URL, headers = kylin_headers)
	if qry_hist_response.status_code != 200 :
                print ('Error : ' + str(qry_hist_response.status_code))
                exit()
	queries = []
	queries = qry_hist_response.json()['data']['query_histories']

	qryCount = qry_hist_response.json()['data']['size'] 

	cntResults = qryCount

	return cntResults

start_time = int(time.time() * 1000)
end_time = (start_time + 10000)
count = funcCounter(start_time,end_time)

while(True):
	newCount = funcCounter(start_time,end_time)
	qps = newCount
	print "################################# \n"
	print "Total Query in last 10 seconds: " + str(qps)
	print "################################# \n"

	start_time = int(time.time() * 1000)
	time.sleep(10)
	end_time = int(time.time() * 1000)
	

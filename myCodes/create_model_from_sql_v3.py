#!/usr/bin/python
###### Author: Saikat Basu, Senior Solution Architect, Kyligence ##########
###### Revision 1 : This script will detect pushdown queries from query history    #############
###### Revision 1 : Then will send create model API call with list of push down queries ######### 
###### Revision 1 : Date: 14th November 2022   ############# 
###### Revision 2 : Added no.of Hrs as configurable parameter; Date: 20th December 2022   ############# 
###### Revision 3 : Modified program adding Optimize Model API call first; Date: Feb 9th 2023  ############# 
###### Revision 3 : If above call makes recommendations, accept those; Date: Feb 9th 2023  ############# 
###### Revision 3 : If no recommendation made for any existing model, then create new models; Date: Feb 9th 2023 ############# 

import requests
import json
import ConfigParser

import sys
import time

config = ConfigParser.ConfigParser()
config.readfp(open(r'cfg_model_create_v3.ini'))

host = config.get('connection', 'host')
port = config.get('connection', 'port')
cfg_project = config.get('connection', 'project')

qry_hist_api = config.get('api', 'qry_hist_api')
model_create_api = config.get('api', 'model_create_api')
model_optimization_api = config.get('api', 'model_optimization_api')
model_accept_api = config.get('api', 'model_accept_api')

numberOfHours = config.get('api', 'numHours')

accept = config.get('header', 'Accept')
accept_language = config.get('header', 'Accept-Language')
content_type = config.get('header', 'Content-Type')
auth = config.get('header', 'Authorization')

kylin_headers = {'Accept': str(accept), 'Accept-Language': str(accept_language), 'Authorization': str(auth), 'Content-Type': str(content_type)}


def pushDownQueries():

	current_time_ms = int( time.time() * 1000 )
	#print str(current_time_ms)

        hours = int(numberOfHours)

	print ("Downloading Query History for " + str(hours) + " hours\n")

	milliseconds = (hours*3600000)
	#print ("milliseconds: " +  str(milliseconds))

	one_day_before_ms = current_time_ms - milliseconds
	#print (str(one_day_before_ms))

	QRY_HIST_URL = host + ":" + port + "/" + qry_hist_api + "?project=" + cfg_project + "&page_size=1000&limit=1000&start_time_from=" + str(one_day_before_ms) + "&start_time_to=" + str(current_time_ms) 

	#print ("Sending Query History URL ==> " + QRY_HIST_URL + "\n")

	qry_hist_response = requests.get(QRY_HIST_URL, headers = kylin_headers)
	#print (str(qry_hist_response))

	#print "Response eeceived for query history \n"
	#print "################################# \n"
	#print "Number of queries in history: " + str(qry_hist_response.json()['data']['size'])
	#print "################################# \n"
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

if (len(pdQueries) == 0):
	print ("There is no pushdown queries.....Existing...")
	sys.exit()

print ("Number of Push Down Queries : " + str(len(pdQueries)))

def optimizeModels(list_of_push_down_queries):
        model_optimization_url = host + ":" + port + "/" + model_optimization_api
        #print (model_optimization_url + "\n")
        url_body = {
                        "project":cfg_project,
                        "sqls":list_of_push_down_queries
                   }
        #print (str(url_body) + "\n")

        print ("Sending Model optimization URL ==> " + model_optimization_url + "\n")
        model_optimization_response = requests.post(model_optimization_url, json = url_body, headers = kylin_headers)

	opt_response_code = model_optimization_response.json()['code']
        print ("API Response Received, Response code ==> " + str(opt_response_code))

        if (str(opt_response_code) != '000'):
                print ("Model Optimization API fails")
                sys.exit()

        elif (str(opt_response_code) == '000'):
                print ("Checking recommendations")

        #print str(model_optimization_response.json())
	models = []
        models = model_optimization_response.json()['data']['models']

	newModels = []

	for model in models:
		totalRecs = 0
		indexRecs = 0
		measureRecs = 0
		ccRecs = 0
		dimRecs = 0

		rec_items = model['recommendations']
		uuid = model['uuid']
		alias = model['alias']
		modelName = str(alias)

		#project = rec_items['project']

		print ("UUID: " + str(uuid) + ", ALIAS: " + modelName)
		#print ("RECOMMENDATIONS : \n")
		#print str(rec_items)
		index_rec = rec_items['index_recommendations']

		#print (str(index_rec))

		indexRecs = len(index_rec)
		#print (str(indexRecs) + "\n")

		measure_rec = rec_items['measure_recommendations']

		#print (str(measure_rec))

		measureRecs = len(measure_rec)
		#print (str(measureRecs) + "\n")

		cc_rec = rec_items['cc_recommendations']
		#print (str(cc_rec))

		ccRecs = len(cc_rec)
		#print (str(ccRecs) + "\n")

		dim_rec = rec_items['dimension_recommendations']
		#print (str(dim_rec))

		dimRecs = len(dim_rec)
		#print (str(dimRecs) + "\n")

		totalRecs = (indexRecs + measureRecs + ccRecs + dimRecs)
		print ("Total recommendations made for model : " + str(alias) +" ==> " + str(totalRecs))

		if (totalRecs > 0):
			print ("Recommendations made for model: " + modelName + "\n")
			newModels.append(modelName)

	return newModels

def acceptRecommendations(myProject, list_of_models):
	model_accept_url = host + ":" + port + "/" + model_accept_api
        print ("Recommendations accept url " + model_accept_url + "\n")
        url_body = {
                        "project":myProject,
			"filter_by_models":"true",
			"model_names":list_of_models
                   }
        print ("URL body : " +str(url_body) + "\n")

        print ("Sending Recommendations Accept URL ==> " + model_accept_url + "\n")
        model_accept_response = requests.put(model_accept_url, json = url_body, headers = kylin_headers)
        #print str(model_accept_response.json())

	acc_response_code = model_accept_response.json()['code']
        print ("API Response Received, Response code ==> " + str(acc_response_code))
        if (str(acc_response_code) != '000'):
                print ("Recommendation accept API fails")
                return False
        elif (str(acc_response_code) == '000'):
                print ("Recommendation accept API succeeds")
                return True


def createModels(list_of_push_down_queries):
	model_create_url = host + ":" + port + "/" + model_create_api
        #print (model_create_url + "\n")
	url_body = {
			"project":cfg_project,
			"sqls":list_of_push_down_queries
		   }
	#print (str(url_body) + "\n")

	print ("Sending Model Create URL ==> " + model_create_url + "\n")
	model_create_response = requests.post(model_create_url, json = url_body, headers = kylin_headers)
        print (str(model_create_response.json()) + "\n")
	
	model_response_code = model_create_response.json()['code']
	print ("Model Create API Response Code ==> " + str(model_response_code) + "\n")
	print ("API Response Received, Parsing Response")

	if (str(model_response_code) != '000'):
		print ("Model create API fails")
		return False
	elif (str(model_response_code) == '000'):
		print ("Model create API succeeds")
		return True

optimized_models = []
optimized_models = optimizeModels(pdQueries)

if (len(optimized_models) > 0):
	acceptedRecs = acceptRecommendations(cfg_project, optimized_models)

	if (str(acceptedRecs) == 'True'):
		print ("Build optimized models and try your queries")
		print ("If you still find queries going to pushdown, run this program again")
		sys.exit()
	elif (str(acceptedRecss) == 'False'):
		print ("Recommendations could not be accepted for some reason")
		print ("Please manually accept recommendations and then build models and try your queries") 
		sys.exit()
elif (len(optimized_models) == 0):
	print ("No recommendation made by model optimize API")
	print ("So attempting to create new models for pushdown queris")

	model_create = createModels(pdQueries)

	if (str(model_create) == 'True'):
		print ("Model Created Successfully")
		print ("Build new models and try your queries")
	elif (str(model_create) == 'False'):
		print ("Neither recommendations made for pushdown queries. Nor new models created for those")
		print ("Sorry .....We need to manually analyze the situation") 
sys.exit()

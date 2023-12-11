#!/usr/bin/python
###### Author: Saikat Basu, Senior Solution Architect, Kyligence ##########
###### Revision 1 :  Submitting Index Build Job; Date: 29th August 2022 ###########
###### Revision 2 :  Modified program to look for only pending or running jobs before submitting a new job #######
###### Revision 2 :  Added more print statements for trouble-shooting ##########
###### Revision 2 :  Also changed it's cfg file for checking all available job status ############
###### Revision 2 :  Changes made on 2nd February 2023 #####################
###### Revision 3 :  Fixed the script; also made it KC compatible for UBS; originally it was for KE ###########
###### Revision 3 :  Jobs submitted for ONLINE models only ##################
###### Revision 3 :  Changes made on 9th May 2023 ##################

import requests
import json

import ConfigParser

import time
from sys import exit

config = ConfigParser.ConfigParser()
config.readfp(open(r'cfg_jobsubmission.ini'))

host = config.get('connection', 'host')
port = config.get('connection', 'port')
workspace = config.get('connection', 'workspace')
project = config.get('connection', 'project')

model_api = config.get('api', 'model_api')
job_api = config.get('api', 'job_api')

ws_prefix = "workspaces/" + workspace
#print ("WS_PREFIX : " + ws_prefix)

accept = config.get('header', 'Accept')
accept_language = config.get('header', 'Accept-Language')
content_type = config.get('header', 'Content-Type')
auth = config.get('header', 'Authorization')

kylin_headers = {'Accept': str(accept), 'Accept-Language': str(accept_language), 'Authorization': str(auth), 'Content-Type': str(content_type)}

def getModels():

	MODEL_API_URL = host + ":" + port + "/" + ws_prefix + "/" + model_api + "?project=" + project
        print ("Sending Get Model List API: " + MODEL_API_URL + "\n")
        
	model_api_response = requests.get(MODEL_API_URL, headers = kylin_headers)
	#print ("Model List Response: \n")
	#print str(model_api_response.json())

	if (model_api_response.status_code != 200):
		print (" Get model list API call failed, exiting.....")
		exit()
	models = []
	models = model_api_response.json()['data']['value']

	model_names = []

	for model in models:
		#print ("Model status ==> " + str(model['status']))
		if (str(model['status']) != 'OFFLINE'):
			model_name = str(model['name'])
			model_names.append(model_name)
		else:
			continue
	print("List of online models: \n")
	print (str(model_names))
	return model_names

myModels = getModels()
model_count = len(myModels)
print ("Number of online models = " + str(model_count))

def submitJob(model_name):
	
	BUILD_API_URL = host + ":" + port + "/" + ws_prefix + "/" + model_api + "/" + model_name + "/segments"
        print("Sending Job Submission API: " + BUILD_API_URL + "\n")
        
	url_body = {"project": project}
	build_api_response = requests.post(BUILD_API_URL, json = url_body, headers = kylin_headers)
	#print (str(build_api_response.json()))

	if (build_api_response.status_code != 200):
		print ("Model load API call failed,exiting ....")
		exit() 
        elif (build_api_response.status_code == 500):
		print ("Model load API call failed, may be some segments are LOCKED.....")
		exit()
	jobs = build_api_response.json()['data']['jobs']
	#print ("Jobs ==> "+ str(len(jobs)))
	if (len(jobs) > 0):
		print "Job submitted successfully"
	else:
		print "Job submission failed"
		exit()

def getJobs():
	current_status = 'done'
	JOB_API_URL = host + ":" + port + "/" + ws_prefix + "/" + job_api + "&project=" + project
	print ("Sending Get Job List API: " + JOB_API_URL + "\n")	
	job_api_response = requests.get(JOB_API_URL, headers = kylin_headers)
	#print ("Received Job API response.......")
	
	#print ("Job API Response ==> " + str(job_api_response.json()))
	
	if (job_api_response.status_code != 200):
		print ("Job list API call failed, exiting....")
		exit()

	jobs = []
	jobs = job_api_response.json()['data']['value']
	#print ("Number of jobs ==> "+str(len(jobs)))
	if len(jobs) > 0:
		for job in jobs:
			status = job['job_status']
			#print ("Status ==> " + status)
			if ((status == 'PENDING') or (status == 'RUNNING')):
				current_status = 'running'
				break
			else:
				continue
		if (current_status == 'done'):
			return True
		elif (current_status == 'running'):
			return False
	else:
		print ("No job found")
		return True

for myModel in myModels:

	while True:
		print ("Checking running job status......")
		time.sleep(10)
		ret = getJobs()
		if ret == False:
			print ("Jobs running, waiting for a minute before checking again..")
			time.sleep(60)
			continue
		elif ret == True:
			break
	print ("Submitting data load job for model ==> " + (str(myModel)))
	submitJob(str(myModel))

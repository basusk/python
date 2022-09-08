#!/usr/bin/python
###### Author: Saikat Basu, Senior Solution Architect, Kyligence ##########
###### Revision 1 :  Submitting Index Build Job; Date: 29th August 2022 ###########

import requests
import json

import ConfigParser

import time
from sys import exit

config = ConfigParser.ConfigParser()
config.readfp(open(r'cfg_jobsubmission.ini'))

host = config.get('connection', 'host')
port = config.get('connection', 'port')
#workspace = config.get('connection', 'workspace')
project = config.get('connection', 'project')

model_api = config.get('api', 'model_api')
job_api = config.get('api', 'job_api')

accept = config.get('header', 'Accept')
accept_language = config.get('header', 'Accept-Language')
content_type = config.get('header', 'Content-Type')
auth = config.get('header', 'Authorization')

kylin_headers = {'Accept': str(accept), 'Accept-Language': str(accept_language), 'Authorization': str(auth), 'Content-Type': str(content_type)}

def getModels():

	MODEL_API_URL = host + ":" + port + "/" + model_api + "?project=" + project

	model_api_response = requests.get(MODEL_API_URL, headers = kylin_headers)
	if (model_api_response.status_code != 200):
		print (" Get model list API call failed, exiting.....")
		#exit()
	print (str(model_api_response))
	models = []
	models = model_api_response.json()['data']['value']

	model_names = []

	for model in models:
		model_name = str(model['name'])
		model_names.append(model_name)

	return model_names

myModels = getModels()
model_count = len(myModels)
print ("Number of models = " + str(model_count))

def submitJob(model_name):
	
	BUILD_API_URL = host + ":" + port + "/" + model_api + "/" + model_name + "/segments"
	url_body = {"project": project}
	build_api_response = requests.post(BUILD_API_URL, json = url_body, headers = kylin_headers)
	if (build_api_response.status_code != 200):
		print ("Model load API call failed,exiting ....")
		#exit() 
	print (str(build_api_response))
	jobs = build_api_response.json()['data']['jobs']
	#print ("Jobs ==> "+ str(len(jobs)))
	if (len(jobs) > 0):
		print "Job submitted successfully"
	else:
		print "Job submission failed"
		#exit()

def getJobs():
	current_status = 'done'
	JOB_API_URL = host + ":" + port + "/" + job_api
	job_api_response = requests.get(JOB_API_URL, headers = kylin_headers)
	#if (job_api_response != 200):
	#	print ("Job list API call failed, exiting....")
#	exit()
	jobs = []
	jobs = job_api_response.json()['data']['value']
	#print ("Length of jobs ==> "+str(len(jobs)))
	if len(jobs) > 0:
		for job in jobs:
			status = job['job_status']
			#print ("Status ==> " + status)
			if status != 'FINISHED':
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
		time.sleep(150)
		ret = getJobs()
		if ret == False:
			print ("Jobs running, waiting ...")
			time.sleep(120)
			continue
		elif ret == True:
			break
	print ("Submitting data load job for model ==> " + (str(myModel)))
	submitJob(str(myModel))

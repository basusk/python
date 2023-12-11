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
###### Revision 4 :  For Metlife, modified the script to monitor job status for the last submitted job ##################
###### Revision 4 :  Earlier script was exiting after submitting last job without waiting for its completion##################
###### Revision 4 :  Also added sys exit code 1 for all the error exit points ##################
###### Revision 4 :  And sys exit code 0 for successful completion  ##################
###### Revision 4 :  Added time stamp in all print statements ##################
###### Revision 4 :  Changes made on 19th July 2023 ##################
###### Revision 5 :  Modified sequential job submission workflow to parallel job submission ##################
###### Revision 5 :  This program submits full data load job for online models at the same time ##################
###### Revision 5 :  With each job submission, it stores job id of submitted jobs from API response ##################
###### Revision 5 :  After submitting all jobs, it keeps on checking status of all submitted jobs via job ids ##################
###### Revision 5 :  When all jobs complete, it exits with exit code 0 ##################
###### Revision 5 :  Changes made on 28th July 2023 ##################

import requests
import json

import ConfigParser

import time
import datetime
import sys

config = ConfigParser.ConfigParser()
config.readfp(open(r'cfg_jobsubmission.ini'))

host = config.get('connection', 'host')
port = config.get('connection', 'port')
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
        print (str(datetime.datetime.now()) + " Sending Get Model List API: " + MODEL_API_URL + "\n")
        
	model_api_response = requests.get(MODEL_API_URL, headers = kylin_headers)
	#print (str(datetime.datetime.now()) +" Model List Response: \n")

	if (model_api_response.status_code != 200):
		print (str(datetime.datetime.now())+ " Get model list API call failed, exiting.....")
		sys.exit(1)
	models = []
	models = model_api_response.json()['data']['value']

	model_names = []

	for model in models:
		#print (str(datetime.datetime.now())+" Model status ==> " + str(model['status']))
		if (str(model['status']) != 'OFFLINE'):
			model_name = str(model['name'])
			model_names.append(model_name)
		else:
			continue
	print(str(datetime.datetime.now())+  " List of online models: \n")
	print (str(datetime.datetime.now())+ " " +str(model_names))
	return model_names

myModels = getModels()
model_count = len(myModels)
print (str(datetime.datetime.now()) + " Number of online models = " + str(model_count))

def submitJob(model_name):
	
#	jobids = []
	BUILD_API_URL = host + ":" + port + "/" + model_api + "/" + model_name + "/segments" 
        print (str(datetime.datetime.now())+ " Sending Job Submission API: " + BUILD_API_URL + "\n")
        
	url_body = {"project": project}
	#build_api_response = requests.post(BUILD_API_URL, json = url_body, headers = kylin_headers)
	build_api_response = requests.post(BUILD_API_URL, data = json.dumps(url_body), headers = kylin_headers)
	#print(build_api_response.json()['data'])

	if (build_api_response.status_code != 200):
		print (str(datetime.datetime.now())+" Job submission API call failed,response no ok,exiting ....")
		sys.exit(1) 
        elif (build_api_response.status_code == 500):
		print (str(datetime.datetime.now()) + " Job submission API call failed,internal server error or may be some segments are locked,exiting....")
		sys.exit(1)
	jobs = build_api_response.json()['data']['jobs']
	print (str(datetime.datetime.now()) + " Jobs ==> "+ str(len(jobs)))
	numberofjobs = len(jobs)
	if (numberofjobs > 0):
		print (str(datetime.datetime.now())+" Job submitted successfully")
		for job in jobs:
			jobid = str(job['job_id'])
			print (str(datetime.datetime.now())+ "Job ID: " + jobid)
#		        jobids.append(jobid)
	else:
		print (str(datetime.datetime.now())+" Job submission failed")
		sys.exit(1)
	#print (str(datetime.datetime.now())+ " JOB IDS: " +str(jobids))
	return jobid


def getJobs(jobId):
	current_status = 'done'
	JOB_API_URL = host + ":" + port + "/" + job_api + "&project=" + project + "&key=" + jobId
	print (str(datetime.datetime.now())+ " Sending Get Job List API: " + JOB_API_URL + "\n")	
	job_api_response = requests.get(JOB_API_URL, headers = kylin_headers)
	#print (str(datetime.datetime.now())+ " Received Job API response.......")
	
	#print (str(datetime.datetime.now())+ " Job API Response ==> " + str(job_api_response.json()))
	
	if (job_api_response.status_code != 200):
		print (str(datetime.datetime.now())+" Get job list API call failed, exiting....")
		sys.exit(1)

	jobs = []
	jobs = job_api_response.json()['data']['value']
	#print (str(datetime.datetime.now())+ " Number of jobs ==> "+str(len(jobs)))
	if len(jobs) > 0:
		for job in jobs:
			status = job['job_status']
			#print (str(datetime.datetime.now())+ " Status ==> " + status)
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
		print (str(datetime.datetime.now())+ " No job found")
		return True

no_of_models = len(myModels)
print(str(datetime.datetime.now())+ " Number of models: " + str(no_of_models))
#loop_counter = no_of_models

submitted_jobids = []
for myModel in myModels:

	print (str(datetime.datetime.now())+ " Submitting data load job for model ==> " + (str(myModel)))
	myJob = submitJob(str(myModel))
	submitted_jobids.append(myJob)

print (str(datetime.datetime.now())+ " SUBMITTED JOB IDS: " + str(submitted_jobids))

print (str(datetime.datetime.now())+ " Checking running job status......")
time.sleep(30)

for id in submitted_jobids:
	while True:
		print (str(datetime.datetime.now())+ "Checking status for job with job id: " + str(id))
       		ret = getJobs(str(id))
       		if ret == False:
        		print (str(datetime.datetime.now())+ " Job running, waiting for 3 minutes before checking again..")
                	time.sleep(180)
                	continue
		elif ret == True:
			print (str(datetime.datetime.now())+ "Job id: "+str(id)+" completed")
			break
	
print (str(datetime.datetime.now())+ " All jobs completed successfully, exiting......")

sys.exit(0)

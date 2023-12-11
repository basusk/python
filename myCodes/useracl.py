#!/usr/bin/python
###### Author: Saikat Basu, Senior Solution Architect, Kyligence ##########
###### Revision 1 : Getting user list from KC ; Date: 12th Mar 2021  ######
###### Revision 2 : Sending user acl url for each user in the list ; Date: 15th Mar 2021 ######
###### Revision 3 : Completed JSON parser for each user's detail ACL with row level filtering information ; Date: 26th Mar 2021 ######

import requests
import json

import ConfigParser

config = ConfigParser.ConfigParser()
config.readfp(open(r'cfg_useracl.ini'))

host = config.get('connection', 'host')
port = config.get('connection', 'port')
workspace = config.get('connection', 'workspace')
project = config.get('connection', 'project')

user_api = config.get('api', 'user_api')
acl_api = config.get('api', 'acl_api')

accept = config.get('header', 'Accept')
accept_language = config.get('header', 'Accept-Language')
content_type = config.get('header', 'Content-Type')
auth = config.get('header', 'Authorization')

USER_URL = host + ":" + port + "/" + user_api

kylin_headers = {'Accept': str(accept), 'Accept-Language': str(accept_language), 'Authorization': str(auth), 'Content-Type': str(content_type)}

print "Sending URL ==> " + USER_URL + "\n"

response = requests.get(USER_URL, headers = kylin_headers)

print "Response received, validating response \n"

if response.status_code != 200 :
	print ('Error : ' + str(response.status_code))
	exit()
else :
	print ('Status Code : ' + str(response.status_code) + ' Status : SUCCESS')

print "Response : \n"
print (response.json())
print "  \n"

print "code : " + response.json()['code'] +"\n"
print "msg : " + response.json()['msg'] + "\n"

users_numbers = response.json()['data']['total_size']
#print "Number of users : " + str(users_numbers)

users = []

for value in response.json()['data']['value'] :
	user = value['username']
	users.append(user)

user_count = len(users)
print "User count : " + str(user_count) + "\n"
if user_count == users_numbers :
	print "Users list ==> " + str(users) + "\n"
else :
	print "Program Failed. There is mismatch in user count" + "\n"

for i in range(user_count):
	
	databases = []
	tables = []
	columns = []
	rows = []

	print "User"+str(i+1)+" ==> "+str(users[i])
	ACL_URL = host + ":" + port + "/" + "workspaces/" + workspace + "/" + acl_api + "/" + users[i] + "?authorized_only=true&project=" + project

	print "Sending URL ==> " + ACL_URL + "\n"

	acl_response = requests.get(ACL_URL, headers = kylin_headers)
	
	print "Response received for " + users[i] +", validating response \n"

	if acl_response.status_code != 200 :
        	print ('Error : ' + str(response.status_code))

        	exit()
	else :
        	print ('Status Code : ' + str(response.status_code) + ' Status : SUCCESS')

		#print "Response Code : "+ acl_response.json()['code']
		#print "Response Msg  : "+ acl_response.json()['msg']
		#print "  \n"
		
		data = []
		data = acl_response.json()['data']
		
		for d in data:
			db = d['database_name']
			databases.append(db)
			tables = []
			tbls = d['tables']
			for tbl in tbls :
				table = tbl['table_name']
				tables.append(table)
				filter_columns = []
				filter_values = []
				for row in tbl['rows'] :
					filter = row['column_name']
					filter_columns.append(filter)
					filter_value = row['items']
					filter_values.append(filter_value)
					#print "Filter value : " + str(filter_value)
					#print "Length of filter values ==> " + str(filter_values_length)	
				#print "Number of columns used for row-level filtering : " + str(len(filter_columns))
				if len(filter_columns) > 0 :
					print "For table " + str(table) + " row level filters set for columns ==> " + str(filter_columns) 
					print "For table " + str(table) + " row level filter values respectively ==> " + str(filter_values) 
			print "User : " + users[i] +" has access to tables ==> " + str(tables) + " in database ==> " + str(db)
		print "User : " + users[i] + " has access to databases " + str(databases) + "\n"

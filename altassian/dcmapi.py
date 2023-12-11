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

#host = config.get('connection', 'host')
host = 'http://20.253.153.229'

#port = config.get('connection', 'port')
port = '8079'

#workspace = config.get('connection', 'workspace')
workspace = 'azureblob'

dcm_api='http://20.253.153.229:8079/workspaces/azureblob/kylin/api/query?project=saikatProj1'


accept = config.get('header', 'Accept')
accept_language = config.get('header', 'Accept-Language')
content_type = config.get('header', 'Content-Type')
auth = config.get('header', 'Authorization')

kylin_headers = {'Accept': str(accept), 'Accept-Language': str(accept_language), 'Authorization': str(auth), 'Content-Type': str(content_type)}

project="saikatProj1"
sql = "select * from P_LINEORDER where LO_SHIPMODE='TRUCK' "

url_body = {
	"project":project,
	"sql":sql }

print "Sending URL ==> " + dcm_api + "\n"

response = requests.post(dcm_api,data=url_body,headers = kylin_headers)


print "Response : \n"
print response
print "  \n"

#print "code : " + response.json()['code'] +"\n"
#print "msg : " + response.json()['msg'] + "\n"

#users_numbers = response.json()['data']['total_size']
#print "Number of users : " + str(users_numbers)

#users = []

#for value in response.json()['data']['value'] :
#	user = value['username']
#	users.append(user)

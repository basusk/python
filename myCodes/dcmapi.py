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
host = 'http://104.40.52.117'

#port = config.get('connection', 'port')
port = '8079'

#workspace = config.get('connection', 'workspace')
workspace = 'saikattstest'

#project = config.get('connection', 'project')
project = 'projectTS'

#dcm_api = config.get('api', 'dcm_api')

dcm_api='http://104.40.52.117:8079/api/spark_source/workspaces/saikattstest/execute'


accept = config.get('header', 'Accept')
accept_language = config.get('header', 'Accept-Language')
content_type = config.get('header', 'Content-Type')
auth = config.get('header', 'Authorization')

kylin_headers = {'Accept': str(accept), 'Accept-Language': str(accept_language), 'Authorization': str(auth), 'Content-Type': str(content_type)}

url_body = {
	"database":"SSB",
	"sql":"create table tab1(colA int)"}


print "Sending URL ==> " + dcm_api + "\n"

response = requests.get(dcm_api,data=url_body,headers = kylin_headers)


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

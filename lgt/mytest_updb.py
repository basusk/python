#!/usr/bin/python

import json
import time
from sys import exit

file = open('updb_cube.json')
data = json.load(file)

dimensions = []

dimensions = data['Create']['ObjectDefinition']['Cube']['Dimensions']['Dimension']
num = len(dimensions)
print ("No. of dimensions = " + str(len(dimensions)))

for dim in dimensions:
	dimId = dim['DimensionID']
	dimName = dim['Name']
	print ("DimensionId: " + dimId + "; DimensionName: " + dimName )

json_data = json.dumps(data)

#print(json_data)


#json_data = '[{"ID":10,"Name":"Pankaj","Role":"CEO"},' \
#            '{"ID":20,"Name":"David Lee","Role":"Editor"}]'

json_object = json.loads(json_data)

json_formatted_str = json.dumps(json_object, indent=2)

#print(json_formatted_str)

#!/usr/bin/python

import json
import time
from sys import exit

file = open('peic.json')
data = json.load(file)

dimensions = []

dimensions = data['Create']['ObjectDefinition']['Database']['Dimensions']['Dimension']
num = len(dimensions)
#print ("No. of dimensions = " + str(len(dimensions)))

for dim in dimensions:
	dimId = dim['ID']
	dimName = dim['Name']
#	print ("DimensionId: " + dimId + "; DimensionName: " + dimName )

cubeDimensions = data['Create']['ObjectDefinition']['Database']['Cubes']['Cube']['Dimensions']['Dimension']
cubeDimNum = len(cubeDimensions)
print ("No. of Cube Dimensions = " + str(cubeDimNum))

for cube in cubeDimensions:
	cubeDimensionID = cube['ID']
	cubeDimensionName = cube['Name']
	print ("CubeDimensionId: " + cubeDimensionID + "; CubeDimensionName: " + cubeDimensionName)


json_data = json.dumps(data)

#print(json_data)


#json_data = '[{"ID":10,"Name":"Pankaj","Role":"CEO"},' \
#            '{"ID":20,"Name":"David Lee","Role":"Editor"}]'

json_object = json.loads(json_data)

json_formatted_str = json.dumps(json_object, indent=2)

#print(json_formatted_str)

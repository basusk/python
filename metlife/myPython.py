#!/usr/bin/python

import sys
import datetime

a = 1
print (str(datetime.datetime.now())+ " a = 1")
b = 2
print (str(datetime.datetime.now())+ " b = 1")

c = a + b
print (str(datetime.datetime.now())+ " c = 1")

if c == 5:
	sys.exit(0)
sys.exit(1)

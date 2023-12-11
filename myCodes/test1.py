#!/usr/bin/python
###### Author: Saikat Basu, Senior Solution Architect, Kyligence ##########
###### Revision 1 : Query load calculator; Date: 13th June  2022 ###########
###### Revision 2 :
###### Revision 3 :

import requests
import json

import ConfigParser

import time

def return_list():
   my_list = []
   for x in range(5):
       my_list.append(x)
   return my_list

list = []
list = return_list()

print list

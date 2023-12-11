import json 
from json2xml import json2xml
with open('UPDB-Cube.xmla') as xmlafile: 
    data = json.load(xmlafile) 
print(json2xml.Json2xml(data).to_xml())

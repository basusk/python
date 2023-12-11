import olap.xmla.xmla as xmla

p = xmla.XMLAProvider()

# mondrian
c = p.connect(location="UPDB-Cube.xmla")

# to analysis services (if iis proxies requests at /olap/msmdpump.dll)
# you will need a valid kerberos principal of course
# c = p.connect(location="https://my-as-server/olap/msmdpump.dll",
#               sslverify="/path/to/my/as-servers-ca-cert.pem")
# to icCube
# c = p.connect(location="http://localhost:8282/icCube/xmla", username="demo",
#               password="demo")

# getting info about provided data
print c.getDatasources()
print c.getMDSchemaCubes()

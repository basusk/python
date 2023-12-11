# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from pprint import pprint
import logging

from sqlalchemy import create_engine, inspect

logging.basicConfig(level=logging.DEBUG)


kylin = create_engine('kylin://SAIKAT:KYLIN!123@localhost:7070/myProj1?version=v4')
pprint(kylin.table_names())

insp = inspect(kylin)
pprint(insp.get_schema_names())
pprint(insp.get_columns('CUSTOMER', 'SSB'))

sql1 = """
select
    "CUSTOMER"."C_MKTSEGMENT" as "c0",
    "CUSTOMER"."C_REGION" as "c1",
    "CUSTOMER"."C_NATION" as "c2",
    sum("LINEORDER"."LO_QUANTITY") as "m0",
    sum("LINEORDER"."LO_REVENUE") as "m1",
    sum("LINEORDER"."LO_SUPPLYCOST") as "m2"
from
    "SSB"."LINEORDER" as "LINEORDER" left join "SSB"."CUSTOMER" as "CUSTOMER" on "LINEORDER"."LO_CUSTKEY" = "CUSTOMER"."C_CUSTKEY"
group by
    "CUSTOMER"."C_MKTSEGMENT",
    "CUSTOMER"."C_REGION",
    "CUSTOMER"."C_NATION"
order by
    "CUSTOMER"."C_MKTSEGMENT" ASC,
    "CUSTOMER"."C_REGION" ASC,
    "CUSTOMER"."C_NATION" ASC
LIMIT 5
"""  # noqa

sql2 = """
SELECT 
DATES.D_YEAR, DATES.D_DATE,count(1)
FROM 
"SSB"."LINEORDER" as "LINEORDER" 
LEFT JOIN "SSB"."CUSTOMER" as "CUSTOMER" ON "LINEORDER"."LO_CUSTKEY"="CUSTOMER"."C_CUSTKEY"
LEFT JOIN "SSB"."DATES" as "DATES" ON "LINEORDER"."LO_ORDERDATE"="DATES"."D_DATEKEY"
LEFT JOIN "SSB"."PART" as "PART" ON "LINEORDER"."LO_PARTKEY"="PART"."P_PARTKEY"
LEFT JOIN "SSB"."SUPPLIER" as "SUPPLIER" ON "LINEORDER"."LO_SUPPKEY"="SUPPLIER"."S_SUPPKEY"
group by CUSTOMER.C_NATION, CUSTOMER.C_CITY, CUSTOMER.C_REGION, DATES.D_YEAR, DATES.D_DATE, PART.P_BRAND
limit 5
"""

sql3 = """
select
    "CUSTOMER"."C_MKTSEGMENT" as "c0",
    "CUSTOMER"."C_REGION" as "c1",
    "CUSTOMER"."C_NATION" as "c2",
    sum("LINEORDER"."LO_QUANTITY") as "m0",
    sum("LINEORDER"."LO_REVENUE") as "m1",
    sum("LINEORDER"."LO_SUPPLYCOST") as "m2"
from
    "SSB"."LINEORDER" as "LINEORDER" left join "SSB"."CUSTOMER" as "CUSTOMER" on "LINEORDER"."LO_CUSTKEY" = "CUSTOMER"."C_CUSTKEY"
group by
    "CUSTOMER"."C_MKTSEGMENT",
    "CUSTOMER"."C_REGION",
    "CUSTOMER"."C_NATION"
order by
    "CUSTOMER"."C_MKTSEGMENT" ASC,
    "CUSTOMER"."C_REGION" ASC,
    "CUSTOMER"."C_NATION" ASC
limit 5
"""


for n in range (1000) :
    queries = [sql3, sql2, sql1]
    for x in queries :
        rp = kylin.execute(x, limit=10, offset=0)
        pprint(rp.fetchall)


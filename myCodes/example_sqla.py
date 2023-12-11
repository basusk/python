# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from pprint import pprint
import logging

from sqlalchemy import create_engine, inspect

logging.basicConfig(level=logging.DEBUG)


kylin = create_engine('kylin://ADMIN:Kyligence@2022@localhost:7070/myProj1?version=v4')
pprint(kylin.table_names())

insp = inspect(kylin)
pprint(insp.get_schema_names())
pprint(insp.get_columns('CUSTOMER', 'SSB'))

sql1 = """
SELECT * FROM SSB.CUSTOMER LIMIT 5
"""  # noqa

sql2 = """
SELECT * FROM SSB.PART LIMIT 5
"""

rp1 = kylin.execute(sql1, limit=10, offset=0)
pprint(rp1.fetchall())

rp2 = kylin.execute(sql2, limit=10, offset=0)
pprint(rp2.fetchall())

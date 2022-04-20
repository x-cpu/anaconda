import pyodbc
import pandas as pd
from datetime import datetime
import os
import pysftp


cnxn = pyodbc.connect("Driver={SQL Server};SERVER=mtv-va-sql-1\p922;Database=DVA;UID=webportal;PWD=Cla$$room")

fileDate = datetime.now().strftime("%m%d%Y_%H%M.csv")
outDIR = (r'\\atl-va-fs06\data\software.lib\athena')
filename = os.path.join(outDIR, 'VAGDITPlatform_Complete_' + fileDate)

sql_query = pd.read_sql_query('''exec VA_GDIT_Completed''',cnxn)
df = pd.DataFrame(sql_query)

df.to_csv (filename, index = False)

srv = pysftp.Connection(host="dtsext.exelaonline.com", username="exelarptusr", password="Rie8ieji")

with srv.cd('/VACore/Inventory'):
    srv.put(filename)

srv.close()

raise SystemExit

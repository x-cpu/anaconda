import pyodbc
import pandas as pd
from datetime import datetime, timedelta
import os
import pysftp
import csv
from tabulate import tabulate
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

cnxn = pyodbc.connect("Driver={SQL Server};SERVER=mtv-va-sql-1\p922;Database=DVA;UID=webportal;PWD=Cla$$room")

yesterday = datetime.now() - timedelta(1)
yesterday = yesterday.strftime('%Y-%m-%d')
fileDate = datetime.now().strftime("%m%d%Y_%H%M.csv")
outDIR = (r'\\atl-va-fs06\data\Misc')
filename = os.path.join(outDIR, 'OMPF_Daily_' + fileDate)

sql_query = pd.read_sql_query('''select 'RMNs Uploaded by GDIT' Status, count(*) Total
from RMNCompleted_OMPF where CompleteDate >= dateadd(day,datediff(day,1,GETDATE()),0) 
and CompleteDate < dateadd(day,datediff(day,0,GETDATE()),0)
UNION
select 'DCSIDs Uploaded by GDIT' Status, count(*) Total
from DCSidCompleted_OMPF where CompleteDate >= dateadd(day,datediff(day,1,GETDATE()),0) 
and CompleteDate < dateadd(day,datediff(day,0,GETDATE()),0)''',cnxn)
df = pd.DataFrame(sql_query)

df.to_csv (filename, index = False)

me = 'Exela Automated <atlhome@lason.com>'
password = 'lason123'
server = 'smtprelay.exelaonline.com:25'
you = ['clarissa.hubbard@exelaonline.com', 'tom.redmond@exelaonline.com', 'matthew.marlow@exelaonline.com', 'donald.bendavid@exelaonline.com', 'rebekah.taulbee@exelaonline.com']
you2 = ['sam.momin@exelaonline.com', 'lunnie.smith@exelaonline.com']


text = """
Please find yesterday's (""" + str(yesterday) + """) completed RMN/DCSID counts for OMPF below:
{table}
"""

html = """
<!DOCTYPE html>
<html><body>
<style> 
  table, th, td {{ border: 1px solid black; border-collapse: collapse;  }}
  tr:last-child{{ background-color: yellow}}
  th, td {{ padding: 5px; }}
</style>
<p>Please find yesterday's (""" + str(yesterday) + """) completed RMN/DCSID counts for OMPF below:
{table}
</body></html>
"""

with open(filename) as input_file:
	reader = csv.reader(input_file)
	data = list(reader)

text = text.format(table=tabulate(data, headers="firstrow", tablefmt="grid"))
html = html.format(table=tabulate(data, headers="firstrow", tablefmt="html"))

message = MIMEMultipart(
    "alternative", None, [MIMEText(text), MIMEText(html,'html')])

message['Subject'] = "OMPF Totals " + str(yesterday)
message['From'] = me
message['To'] = ", ".join(you)
message['CC'] = ", ".join(you2)

server = smtplib.SMTP(server)
#server.sendmail(me, you, message.as_string())
server.send_message(message)
server.quit()



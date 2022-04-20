import pyodbc
import pandas as pd
from datetime import datetime
import os
import pysftp
import csv
from tabulate import tabulate
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

cnxn = pyodbc.connect("Driver={SQL Server};SERVER=mtv-va-sql-1\p922;Database=DVA;UID=webportal;PWD=Cla$$room")

fileDate = datetime.now().strftime("%m%d%Y_%H%M.csv")
outDIR = (r'\\atl-va-fs06\data\MonthlyCounts')
filename = os.path.join(outDIR, 'MonthlyCounts_' + fileDate)

sql_query = pd.read_sql_query('''select 
	UploadDate = ISNULL(UploadDate, 'Total'),
	[FCS Docs] = sum(X.[FCS Docs]),
	[FCS Images] = sum(X.[FCS Images]),
	[OMPF Docs] = sum(X.[OMPF Docs]),
	[OMPF Images] = sum(X.[OMPF Images]),
	[RMC Docs] = sum(X.[RMC Docs]),
	[RMC Images] = sum(X.[RMC Images])
FROM
(select distinct T.UploadDate, 
ISNULL(sum(T.[FCS Docs]), 0) 'FCS Docs',
ISNULL(sum(T.[FCS Images]), 0) 'FCS Images',
ISNULL(sum(T.[OMPF Docs]), 0) 'OMPF Docs',
ISNULL(sum(T.[OMPF Images]), 0) 'OMPF Images',
ISNULL(sum(T.[RMC Docs]), 0) 'RMC Docs',
ISNULL(sum(T.[RMC Images]), 0) 'RMC Images'
FROM
(select distinct CONVERT(nvarchar, ftpstime, 101) UploadDate, pbatch,
CASE
	WHEN pbatch like '02%' and not exists (select * from customerCheckIn
	where document.RMN = RMN and claimtype = 'OMPF') Then count(distinct ImageID)
	END 'FCS Docs',
CASE
	WHEN pbatch like '02%' and not exists (select * from customerCheckIn
	where document.RMN = RMN and claimtype = 'OMPF') Then sum(numpages)
	END 'FCS Images',
CASE
	WHEN pbatch like '02%' and exists (select * from customerCheckIn
	where document.RMN = RMN and claimtype = 'OMPF') Then count(distinct ImageID)
	END 'OMPF Docs',
CASE
	WHEN pbatch like '02%' and exists (select * from customerCheckIn
	where document.RMN = RMN and claimtype = 'OMPF') Then sum(numpages)
	END 'OMPF Images',
CASE
	WHEN pbatch like '01%' Then count(distinct ImageID)
	END 'RMC Docs',
CASE
	WHEN pbatch like '01%' Then sum(numpages)
	END 'RMC Images'
from document 
where ftpstime >= CAST(DATEADD(DAY,-DAY(GETDATE())+1, CAST(GETDATE() AS DATE)) AS DATETIME)
and ftpstime < CONVERT(date, getDate())
group by CONVERT(nvarchar, ftpstime, 101), pbatch, RMN) T
group by T.UploadDate) X
group by ROLLUP(UploadDate)''',cnxn)
df = pd.DataFrame(sql_query)

df.to_csv (filename, index = False)

me = 'Exela Automated <atlhome@lason.com>'
password = 'lason123'
server = 'smtprelay.exelaonline.com:25'
you = ['danny.bishop@exelaonline.com', 'robert.searcy@exelaonline.com', 'tim.shields@exelaonline.com', 'stephanie.king@exelaonline.com']
#you = ['sam.momin@exelaonline.com', 'lunnie.smith@exelaonline.com']
you2 = ['sam.momin@exelaonline.com', 'lunnie.smith@exelaonline.com']
#you = ['lunnie.smith@exelaonline.com']
#you2 = ['lunnie.smith@exelaonline.com']

text = """
Please find the latest monthly totals below:
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
<p>Please find the latest monthly totals below:
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

message['Subject'] = "Monthly Totals - FCS OMPF RMC"
message['From'] = me
message['To'] = ", ".join(you)
message['CC'] = ", ".join(you2)

server = smtplib.SMTP(server)
#server.sendmail(me, you, message.as_string())
server.send_message(message)
server.quit()


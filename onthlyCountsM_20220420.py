#!/usr/bin/env python
# coding: utf-8

# In[3]:


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


# In[4]:


cnxn = pyodbc.connect("Driver={SQL Server};SERVER=mtv-va-sql-1\p922;Database=DVA;UID=webportal;PWD=Cla$$room")


# In[5]:


fileDate = datetime.now().strftime("%m%d%Y_%H%M.csv")
outDIR = (r'\\atl-va-fs06\data\MonthlyCounts')
filename = os.path.join(outDIR, 'MonthlyCounts_' + fileDate)


# In[6]:


sql_query = pd.read_sql_query('''select 
	UploadDate = ISNULL(X.UploadDate, 'Total'),
	[FCS Docs] = sum(X.[FCS Docs]),
	[FCS Images] = sum(X.[FCS Images]),
	[OMPF Docs] = sum(X.[OMPF Docs]),
	[OMPF Images] = sum(X.[OMPF Images]),
	[RMC Docs] = sum(X.[RMC Docs]),
	[RMC Images] = sum(X.[RMC Images]),
	[London Docs] = sum(X.[London Docs]),
	[London Images] = sum(X.[London Images])
FROM
(select distinct T.UploadDate, 
ISNULL(sum(T.[FCS Docs]), 0) 'FCS Docs',
ISNULL(sum(T.[FCS Images]), 0) 'FCS Images',
ISNULL(sum(T.[OMPF Docs]), 0) 'OMPF Docs',
ISNULL(sum(T.[OMPF Images]), 0) 'OMPF Images',
ISNULL(sum(T.[RMC Docs]), 0) 'RMC Docs',
ISNULL(sum(T.[RMC Images]), 0) 'RMC Images',
CASE
	WHEN l.docs is null THEN 0
	ELSE l.docs
	END 'London Docs',
CASE
	WHEN l.images is null THEN 0
	ELSE l.images
	END 'London Images'
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
left join LondonMonthly l
on T.UploadDate = l.UploadDate
group by T.UploadDate, l.docs, l.images) X
group by ROLLUP(X.UploadDate)''',cnxn)


# In[7]:


df = pd.DataFrame(sql_query)


# In[8]:


df.to_csv (filename, index = False)
#html_table_blue_light = build_table(df, 'blue_light')
#print(html_table_blue_light)


# In[9]:


me = 'Exela Automated <atlhome@lason.com>'
password = 'lason123'
server = 'smtprelay.exelaonline.com:25'
you = ['danny.bishop@exelaonline.com', 'Kristen.Adams@exelaonline.com', 'John.Blankenship@exelaonline.com', 'stephanie.king@exelaonline.com', 'bala.parasuraman@exelatech.com', 'Michael.Cincinelli@exelaonline.com', 'Katie.Flynn@exelaonline.com', 'Charles.Vaughn@exelaonline.com', 'John.VanWinkle@exelaonline.com', 'Gunasekaran.Ethiraj@exelaonline.com', 'Jeganathan.Balaraman@exelaonline.com', 'Balaraman.Sundaramurthy@exelaonline.com', 'Ranjith.Gunasekaran@exelaonline.com', 'Clarissa.Hubbard@exelaonline.com', 'Santosh.Rudrappa@exelaonline.com', 'Rebecca.Shuart@exelaonline.com']
you2 = ['tom.redmond@exelaonline.com', 'sam.momin@exelaonline.com', 'lunnie.smith@exelaonline.com', 'sasha.wernersbach@exelaonline.com', 'tenny.akihary@exelaonline.com']
#you = ['lunnie.smith@exelaonline.com']
#you2 = ['lunnie.smith@exelaonline.com']


# In[10]:


text = """
Please find the latest monthly totals below:
{table}
"""


# In[11]:


def bold(x): 
    return ['font-weight: bold' if v == 'Total'  else '' for v in x]

def green(val):
    color = 'green' if val > 150 else 'black'
    return 'color: %s' % color

th_props = [
    ('font-family', 'Calibri'),
    ('text-align', 'center'),
    ('font-weight', 'bold')
    ]

# Set CSS properties for td elements in dataframe
td_props = [
    ('font-family', 'Calibri')
    ]


# Set table styles
styles = [
  dict(selector="th", props=th_props),
  dict(selector="td", props=td_props)
  ]

#df_styled = df.style.apply(bold, axis=1)
df_styled = (df.style
                .apply(bold, axis=1)
                #.applymap(green, subset=pd.IndexSlice['RMC Docs'])
                .highlight_max(color = 'yellow', axis = 0)
                .set_table_styles(styles))

df_styled 


# In[12]:


df_styled_html = df_styled.hide_index().render()


# In[13]:


html = """<html>
  <head></head>
  <body>
  <style> 
  table, th, td {{ border: 1px solid black; border-collapse: collapse;  }}
  th, td {{ padding: 5px; }}
</style>
<p style="font-family:calibri;">Please find the latest monthly totals below:
    {0}
  </body>
</html>
""".format(df_styled_html)


# In[14]:


#with open('styled_rendered.html', 'w') as f:
#    f.write(df_styled_html)


# In[15]:


with open(filename) as input_file:
	reader = csv.reader(input_file)
	data = list(reader)


# In[16]:


text = text.format(table=tabulate(data, headers="firstrow", tablefmt="grid"))
#html = html.format(table=tabulate(data, headers="firstrow", tablefmt="html"))


# In[17]:


message = MIMEMultipart(
    "alternative", None, [MIMEText(text), MIMEText(html,'html')])


# In[18]:


message['Subject'] = "Monthly Totals - FCS OMPF RMC"
message['From'] = me
message['To'] = ", ".join(you)
message['CC'] = ", ".join(you2)


# In[19]:


server = smtplib.SMTP(server)
#server.sendmail(me, you, message.as_string())
server.send_message(message)
server.quit()


# In[ ]:





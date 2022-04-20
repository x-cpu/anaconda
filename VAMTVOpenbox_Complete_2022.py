#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyodbc
import pandas as pd
from datetime import datetime
import os
import sqlite3
from sqlalchemy import create_engine
import csv
#import xlsxwriter
import yagmail
import pysftp


# In[2]:


cnxn = pyodbc.connect("Driver={SQL Server};SERVER=mtv-va-sql-1\p922;Database=DVA;UID=webportal;PWD=Cla$$room")


# In[3]:


fileDate = datetime.now().strftime("%m%d%Y_%H%M.csv")
outDIR = (r'\\atl-va-fs06\data\software.lib\athena')
filename = os.path.join(outDIR, 'VAMTVOpenbox_Complete_' + fileDate)


# In[4]:


sql_query01 = pd.read_sql_query('''select distinct T.PBatch, T.RMN, T.ImageCount, T.DocCount,
T.CompleteDate,
CustomerName='VA',
ProcessType='OpenBox',
Status='COMPLETED',
SiteName='MTV',
Vertical='Public Sector',
Region='NA',
Country='US',
BillType='NA',
[Sub-Process]='Upload',
Boxes=0,
xBoxes=1,
Characters=0,
xImageCount =0,
xDocCount=0
FROM
(select distinct d.PBatch, d.RMN, sum(d.NumPages) ImageCount,
count(distinct d.ImageID) DocCount, CONVERT(varchar, d.ftpstime, 101) CompleteDate
--into xdocs
from document d
where d.ftpstime >= '2021-12-01'
and (d.pbatch like '01%' or d.pbatch like '02%' 
or d.pbatch like '07%' or d.pbatch like '08%')
and d.SysKey not in (select syskey from document where ftpstime >'5/1/2021 0:0:0'
and dcsid in (select dcs from xompf5550))
group by d.PBatch, d.RMN, CONVERT(varchar, d.ftpstime, 101)) T''', cnxn)


# In[5]:


sql_query03 = pd.read_sql_query('''select distinct T.PBatch, T.RMN, T.Received, c.claimtype
FROM
(select distinct p.pbatch, p.RMN, CONVERT(varchar, p.InvTime, 101) Received
--into xpbatch
from PhysicalBatch p
where (p.pbatch like '01%' or p.pbatch like '02%' 
or p.pbatch like '07%' or p.pbatch like '08%')
--changed date below on 6/30/21
and p.InvTime >= '2018-08-27') T
left join customercheckin c
on T.RMN = c.RMN''', cnxn)


# In[6]:


engine = create_engine('sqlite://', echo=False)


# In[7]:


df01 = pd.DataFrame(sql_query01)
df03 = pd.DataFrame(sql_query03)


# In[8]:


#df01


# In[9]:


df01.to_sql('doc', engine, if_exists='replace', index=False)
df03.to_sql('pbatch', engine, if_exists='replace', index=False)


# In[10]:


results = engine.execute('''select distinct p.Received,
X.CustomerName,
CASE
	WHEN X.Batchname like '01%' and p.RMN like '101%' then 'VAULT'
	WHEN X.Batchname like '01%' and p.RMN not like '101%' then 'RMC'
	WHEN X.Batchname like '02%' and p.claimtype <> 'OMPF' then 'FCS'
	WHEN X.Batchname like '02%' and p.claimtype = 'OMPF' then 'OMPF'
	WHEN X.Batchname like '07%' then 'BWN'
	WHEN X.Batchname like '08%' then 'REA'
	END JobName,
X.ProcessType,
X.BatchName,
X.Status,
X.ImageCount, X.DocCount,
X.SiteName,
X.CompleteDate,
X.Vertical,
X.Region,
X.Country,
X.BillType,
X.[Sub-Process],
X.Boxes,
X.Characters
FROM
(select distinct d.pbatch Batchname, d.RMN RMN,
d.ImageCount,
d.DocCount,
d.CompleteDate,
d.CustomerName,
d.ProcessType,
d.Status,
d.SiteName,
d.Vertical,
d.Region,
d.Country,
d.BillType,
d.[Sub-Process],
d.Boxes,
d.Characters
from doc d
group by d.pbatch, d.RMN, d.CompleteDate) X
left join pbatch p
on X.Batchname = p.pbatch
where p.Received is not null
UNION
select distinct p.Received,
d.CustomerName,
CASE
	WHEN d.pbatch like '01%' and d.RMN like '101%' then 'VAULT'
	WHEN d.pbatch like '01%' and d.RMN not like '101%' then 'RMC'
	WHEN d.pbatch like '02%' and p.claimtype <> 'OMPF' then 'FCS'
	WHEN d.pbatch like '02%' and p.claimtype = 'OMPF' then 'OMPF'
	WHEN d.pbatch like '07%' then 'BWN'
	WHEN d.pbatch like '08%' then 'REA'
	END JobName,
d.ProcessType,
d.RMN BatchName,
d.Status,
d.xImageCount,
d.xDocCount,
d.SiteName,
d.CompleteDate,
d.Vertical,
d.Region,
d.Country,
d.BillType,
d.[Sub-Process],
d.xBoxes,
d.Characters
from doc d
left join pbatch p
on d.pbatch = p.pbatch
where p.Received is not null
order by JobName, BatchName, CompleteDate''')


# In[11]:


finale = pd.DataFrame(results)
finale.columns = ['Received', 'CustomerName', 'JobName', 'ProcessType', 'BatchName', 'Status', 'ImageCount', 'DocCount', 'SiteName', 'CompleteDate', 'Vertical', 'Region', 'Country', 'BillType', 'Sub-Process', 'Boxes', 'Characters']
#finale


# In[12]:


finale.to_csv (filename, index = False)


# In[ ]:


#srv = pysftp.Connection(host="dtsext.exelaonline.com", username="exelarptusr", password="Rie8ieji")

#with srv.cd('/VACore/Inventory'):
    srv.put(filename)

#srv.close()


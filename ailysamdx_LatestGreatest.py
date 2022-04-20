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
from zipfile import ZipFile
import mF
from os.path import basename
import gobble 
import yagmail
import zipfile


# In[2]:


cnxn = pyodbc.connect("Driver={SQL Server};SERVER=mtv-va-sql-1\p922;Database=DVA;UID=webportal;PWD=Cla$$room")


# In[3]:


#fileDate = datetime.now().strftime("%m%d%Y_%H%M.csv")
#fileDate = datetime.now().strftime("%Y%m%d%H%M%S.csv")
fileDate = datetime.now().strftime("%Y%m%d%H%M%S")
outDIR = (r'\\atl-va-fs06\data\Daily\BVTI Image Review')
filename = os.path.join(outDIR, 'BVTIImageReview-' + fileDate)


# In[4]:


#sql_query01 = pd.read_sql_query('''select distinct
#d.RMN,
#d.dcsID,
#d.ImageID,
#CONVERT(varchar, d.ImageDateTime, 23) 'PDF Created',
#d.NumPages 'Image Count Per PDF',
#c.Firstname xFirstname, c.Lastname xLastname,
#d.FileNumber xFileNumber, d.docidDoctype DocType,
#c.Recvdate 'Date of Receipt',
#cc.claimtype
#from document d
#left join CustomerDATA c
#on d.dcsID = c.dcsID and d.Pbatch = c.Pbatch
#left join customerCheckIn cc
#on d.RMN = cc.RMN
#where d.ftpstime >= CONVERT(varchar, GETDATE(), 23)''', cnxn)


# In[5]:


sql_query01 = pd.read_sql_query('''
select distinct T.RMN, T.dcsID, T.ImageID,
T.[PDF Created],
T.[Image Count Per PDF],
T.DocType, T.[Date of Receipt], T.claimtype
into dvarp.dbo.xbvtiReview
FROM
(select distinct
d.RMN,
d.dcsID,
d.ImageID,
CONVERT(varchar, d.ImageDateTime, 23) 'PDF Created',
d.NumPages 'Image Count Per PDF',
d.docidDoctype DocType,
c.Recvdate 'Date of Receipt',
cc.claimtype
from document d WITH (NOLOCK)
left join CustomerDATA c
on d.dcsID = c.dcsID and d.Pbatch = c.Pbatch
left join customerCheckIn cc
on d.RMN = cc.RMN
where d.ftpstime >= dateadd(day,datediff(day,1,GETDATE()),0)
and d.ftpstime < dateadd(day,datediff(day,0,GETDATE()),0)) T
where T.RMN not in (select distinct p.RMN 
from QAExceptions q
left join PhysicalBatch p
on q.batchname = p.PBatch)
''', cnxn)


# In[6]:


df = pd.DataFrame(sql_query01)


# In[7]:


df.columns = ['RMN', 'dcsID', 'ImageID', 'PDF Created', 'Image Count Per PDF',
       'DocType', 'Date of Receipt',
       'claimtype']


# In[8]:


#df['Veteran Name'] = df['xFirstname'].str[:1] + '*** ' + df['xLastname'].str[:1] + '***'


# In[9]:


#df['FileNumber'] = '********' + df['xFileNumber'].str.strip().str[-1]


# In[10]:


dfx01 = df.loc[:, ['RMN', 'dcsID', 'ImageID', 'PDF Created', 'Image Count Per PDF',
       'DocType', 'Date of Receipt',
       'claimtype']]


# In[11]:


#dfx02 = dfx01.drop(['xFirstname', 'xLastname', 'xFileNumber'], axis=1)


# In[12]:


dfx01.to_csv(filename + '.csv', index = False)


# In[13]:


with ZipFile(filename + '.zip', 'w', zipfile.ZIP_DEFLATED) as zipObj:
    zipObj.write(filename + '.csv', basename(filename + '.csv'))


# In[14]:


fileDate2 = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
body = 'Please find the latest file within attached zip.'


# In[15]:


yag = yagmail.SMTP(user={'atlhome@lason.com': 'Exela Automated'}, password='lason123', 
                   host='smtprelay.exelaonline.com', port=25, 
                   smtp_ssl=False, smtp_starttls=False, smtp_skip_login=True)


# In[16]:


yag.send(
    to=mF.ENormaStits(),
    #to=gobble.deesKnuts(),
    cc=gobble.deesKnuts(),
    subject='BVTI Image Review ' + fileDate2,
    contents=body,
    attachments=filename + '.zip'
    )


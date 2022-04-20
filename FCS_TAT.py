#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyodbc
import pandas as pd
import numpy as np
from datetime import datetime
import os
#import csv
#from tabulate import tabulate
#from email.mime.multipart import MIMEMultipart
#from email.mime.text import MIMEText
#from email.mime.base import MIMEBase
#from email import encoders
import sqlite3
from sqlalchemy import create_engine
import xlsxwriter
#import smtplib
import yagmail


# In[2]:


cnxn = pyodbc.connect("Driver={SQL Server};SERVER=mtv-va-sql-1\p922;Database=DVA;UID=webportal;PWD=Cla$$room")


# In[3]:


fileDate = datetime.now().strftime("%m%d%Y_%H%M")
fileDate2 = datetime.now().strftime("%Y%m%d")
outDIR = (r'\\atl-va-fs06\data\FCS\2021')
filename = os.path.join(outDIR, 'FCS_TAT_' + fileDate)


# In[4]:


sql_query03 = pd.read_sql_query('''
select distinct p.RMN, p.Pbatch, p.dcsid, pp.BatchClassName
from PbatchDCSMapping p 
left join PhysicalBatch pp
on p.Pbatch = pp.PBatch
where exists (select * from customerCheckIn where p.RMN = RMN
and claimtype <> 'OMPF' and boxOrigin = 'fcs' and boxSource = 'ro') and p.pbatch like '02%'
and p.invtime >= '2020-01-01'
''', cnxn)


# In[5]:


sql_query = pd.read_sql_query('''select distinct Z.RMN, Z.TrackingNo, Z.claimtype, Z.BatchName,
Z.BatchClassName, Z.dcsid, Z.ScannedBatchName, Z.InvTime,
s.kbatch ExportedBatchName
FROM
(select distinct Y.RMN, Y.TrackingNo, Y.claimtype, Y.BatchName,
Y.BatchClassName, Y.dcsid, s.kbatch ScannedBatchName, Y.InvTime
FROM
(select distinct X.RMN, X.TrackingNo, X.claimtype, X.BatchName,
X.BatchClassName, p.dcsid, X.InvTime
FROM
(select distinct T.RMN, T.TrackingNo TrackingNo, T.claimtype, 
p.PBatch BatchName, p.BatchClassName, p.InvTime
FROM
(select distinct RTRIM(c.RMN) RMN, c.trackingno, c.claimtype
from customerCheckIn c
where c.trackingno is not null
and c.trackingno <> ''
and c.trackingno <> '1234DUMMY1234'
and claimtype <> 'OMPF' and boxOrigin = 'fcs' and boxSource = 'ro') T
left join PhysicalBatch p
on T.RMN = p.RMN
where  p.InvTime >= '2020-06-01' and p.PBatch like '02%') X
left join PbatchDCSMapping p
on X.BatchName = p.Pbatch) Y
left join Stats_IBML s
on Y.BatchName = s.kbatch) Z
left join stats s
on Z.BatchName = s.kbatch''', cnxn)


# In[6]:


sql_query04 = pd.read_sql_query('''
select distinct BatchName, WFStep, BatchLocation, BatchStatus, TotalImages from [mtv-va-sql-4\p1].turboscanng1.dbo.batches b
left join PhysicalBatch p
on b.BatchName = p.PBatch
where exists (select * from customerCheckIn where p.RMN = RMN
and claimtype <> 'OMPF' and boxOrigin = 'fcs' and boxSource = 'ro') and p.PBatch like '02%'
''', cnxn)


# In[7]:


sql_query05 = pd.read_sql_query('''
select distinct BatchName 
from [mtv-va-sql-2\p923].IBMLTEST_Data.dbo.batchtable''', cnxn)


# In[8]:


sql_query02 = pd.read_sql_query('''
select distinct d.RMN, d.Pbatch, d.dcsid, 
--d.ImageID,
CONVERT(nvarchar, max(d.ftpstime), 101) UploadDateTime
--into xoDCSIDsExported
from document d
where exists (select * from customerCheckIn where d.RMN = RMN
and claimtype <> 'OMPF' and boxOrigin = 'fcs' and boxSource = 'ro')
and d.ImageDateTime > '2020-06-01'
and d.PBatch like '02%'
--and exists (select * from document where d.dcsID = dcsid
--and d.PBatch = pbatch and ftpstime is not null)
group by d.RMN, d.Pbatch, d.dcsid
--, d.ImageID
order by UploadDateTime''', cnxn)


# In[9]:


sql_query06 = pd.read_sql_query('''
select * from calendar''', cnxn)


# In[10]:


engine = create_engine('sqlite://', echo=False)


# In[11]:


df = pd.DataFrame(sql_query)
df02 = pd.DataFrame(sql_query02)
df03 = pd.DataFrame(sql_query03)
df04 = pd.DataFrame(sql_query04)
df05 = pd.DataFrame(sql_query05)
df06 = pd.DataFrame(sql_query06)


# In[12]:


df.to_sql('fcsMaster', engine, if_exists='replace', index=False)
df02.to_sql('uDCSIDs', engine, if_exists='replace', index=False)
df03.to_sql('aDCSIDs', engine, if_exists='replace', index=False)
df04.to_sql('tbatches', engine, if_exists='replace', index=False)
df05.to_sql('ibatches', engine, if_exists='replace', index=False)
df06.to_sql('calendar', engine, if_exists='replace', index=False)

results = engine.execute('''select distinct X.RMN, X.TrackingNo, X.ClaimType,
X.Batches, X.DCSIDs,
X.Batches - X.ExportedBatches InProcessBatches,
X.ExportedBatches, X.UploadedBatches,
X.UploadedDCSIDs
FROM
(select distinct T.RMN, T.TrackingNo, T.ClaimType,
count(distinct T.BatchName) Batches,
count(distinct T.DCSID) DCSIDs,
count(distinct T.ExportedBatchName) ExportedBatches,
count(distinct T.UploadedBatch) UploadedBatches,
count(distinct T.UploadedDCSID) UploadedDCSIDs
FROM
(select distinct M.RMN, M.TrackingNo, M.ClaimType,
M.BatchName, M.DCSID, M.ExportedBatchName, 
u.pbatch UploadedBatch, u.dcsid UploadedDCSID
from fcsMaster M
left join uDCSIDs u
on M.BatchName = u.PBatch) T
group by T.RMN, T.TrackingNo) X''')
finale = pd.DataFrame(results)
finale.columns = ['RMN', 'TrackingNo', 'ClaimType', 'TotalBatches', 'TotalDCSIDs', 'InProgress(Batches)', 'Exported(Batches)', 'Uploaded(Batches)', 'Uploaded(DCSIDs)']
#finale


# In[13]:


#finale.head(100)


# In[14]:


finale['Status'] = np.where(finale['TotalBatches'] == finale['Uploaded(Batches)'], True, False)
finale.loc[finale['Status'] == True, 'Status'] = 'Completed'
finale.loc[finale['Status'] == False, 'Status'] = 'Partial'
#finale.head(100)


# In[15]:


results2 = engine.execute('''select distinct T.RMN, T.TrackingNo, T.BatchName, strftime('%Y-%m-%d',T.InvTime) InvTime,
CAST((julianday('now') - julianday(T.InvTime) - (select count(*) from calendar 
where julianday(calendardate) between julianday(T.InvTime) and julianday('now')
and (DayOfWeekName in ('Saturday', 'Sunday') 
or CalendarDateDescription is not null))) As Integer),
T.OpenBoxBatchLocation,
CASE
	When T.BatchStatus = 'Exported' Then 'Pending Upload'
	When T.BatchStatus = 'Pending OpenBox Import' and i.BatchName is null Then 'Pending IBML Scan'
	Else T.BatchStatus
	END BatchStatus, T.TotalImages, T.SpecialMediaoPaper, T.DCSIDPaper, T.DCSIDSM
FROM
(select X.RMN, X.TrackingNo, X.BatchName, X.InvTime,
X.OpenBoxBatchLocation, X.BatchStatus,
X.TotalImages, X.SpecialMediaoPaper, X.DCSIDPaper, X.DCSIDSM,
--count(distinct u.imageID) TotalDocs,
count(distinct u.UploadDateTime) TotalUploaded
FROM
(select distinct M.RMN, M.TrackingNo,
M.BatchName, M.InvTime,
CASE
	WHEN b.WFStep = 1 Then 'Capture'
	WHEN b.WFStep = 2 Then 'Enhance1'
	WHEN b.WFStep = 3 Then 'FOCR'
	WHEN b.WFStep = 4 Then 'Enhance2'
	WHEN b.WFStep = 5 Then 'Separation'
	WHEN b.WFStep = 6 Then 'ImageQC'
	WHEN b.WFStep = 7 Then 'AutoIndex'
	WHEN b.WFStep = 8 Then 'DocID'
	WHEN b.WFStep = 9 Then 'DocIDQC'
	WHEN b.WFStep = 10 Then 'Manual Index'
	WHEN b.WFStep = 11 Then 'Manual IndexQC'
	WHEN b.WFStep = 12 and batchlocation = 64 then 'Verification'
	WHEN b.WFStep = 13 and batchlocation <> 0 then 'Export'  
	END OpenBoxBatchLocation,
CASE
	WHEN M.ExportedBatchName is not null then 'Exported'
	WHEN BatchStatus = 1 Then 'Ready'
	WHEN BatchStatus = 2 Then 'In Process'
	WHEN BatchStatus = 4 Then 'Suspended'
	WHEN BatchStatus = 8 Then 'Auto-Fail'
	WHEN M.ExportedBatchName is null and b.BatchName is null then 'Pending OpenBox Import'	
	END BatchStatus, b.TotalImages,
CASE
	WHEN M.BatchClassName = 'SM' Then 'SM'
	ELSE 'P'
	END SpecialMediaoPaper,
CASE
	WHEN M.BatchClassName <> 'SM' Then count(distinct M.dcsid)
	ELSE 0
	END DCSIDPaper,
CASE 
	WHEN M.BatchClassName = 'SM' Then count(distinct M.dcsid)
	ELSE 0
	END DCSIDSM
from fcsMaster M
left join tbatches b
on M.BatchName = b.BatchName
group by M.RMN, M.TrackingNo, M.BatchName, b.WFStep, b.BatchLocation,
M.ExportedBatchName, b.BatchStatus, b.BatchName, b.TotalImages, M.BatchClassName) X
left join uDCSIDs u
on X.BatchName = u.pbatch
where u.UploadDateTime is null
group by X.RMN, X.TrackingNo, X.BatchName, X.OpenBoxBatchLocation, X.BatchStatus, X.TotalImages,
X.SpecialMediaoPaper, X.DCSIDPaper, X.DCSIDSM) T
left join ibatches i
on T.Batchname = i.BatchName
order by T.BatchName''')
finale2 = pd.DataFrame(results2)
finale2.columns = ['RMN', 'TrackingNo', 'BatchName', 'InvTime', 'TAT(Days)', 'OpenBoxBatchLocation', 'BatchStatus', 'TotalImages(OpenBox)', 'SpecialMedia/Paper', 'Paper(DCSID Count)', 'SpecialMedia(DCSID Count)']
#finale2


# In[16]:


#finale2


# In[17]:


results3 = engine.execute('''select distinct T.RMN, T.TrackingNo, T.BatchName, strftime('%Y-%m-%d',T.InvTime) InvTime, 
CAST((julianday('now') - julianday(T.InvTime) - (select count(*) from calendar 
where julianday(calendardate) between julianday(T.InvTime) and julianday('now')
and (DayOfWeekName in ('Saturday', 'Sunday') 
or CalendarDateDescription is not null))) As Integer),
T.DCSID,
T.OpenBoxBatchLocation,
CASE
	When T.BatchStatus = 'Exported' Then 'Pending Upload'
	When T.BatchStatus = 'Pending OpenBox Import' and i.BatchName is null Then 'Pending IBML Scan'
	Else T.BatchStatus
	END BatchStatus, T.TotalImages, T.SpecialMediaoPaper
FROM
(select X.RMN, X.TrackingNo, X.BatchName, X.InvTime, X.dcsid DCSID,
X.OpenBoxBatchLocation, X.BatchStatus,
X.TotalImages, X.SpecialMediaoPaper,
--count(distinct u.imageID) TotalDocs,
count(distinct u.UploadDateTime) TotalUploaded
FROM
(select distinct M.RMN, M.TrackingNo,
M.BatchName, M.InvTime, M.DCSID,
CASE
	WHEN b.WFStep = 1 Then 'Capture'
	WHEN b.WFStep = 2 Then 'Enhance1'
	WHEN b.WFStep = 3 Then 'FOCR'
	WHEN b.WFStep = 4 Then 'Enhance2'
	WHEN b.WFStep = 5 Then 'Separation'
	WHEN b.WFStep = 6 Then 'ImageQC'
	WHEN b.WFStep = 7 Then 'AutoIndex'
	WHEN b.WFStep = 8 Then 'DocID'
	WHEN b.WFStep = 9 Then 'DocIDQC'
	WHEN b.WFStep = 10 Then 'Manual Index'
	WHEN b.WFStep = 11 Then 'Manual IndexQC'
	WHEN b.WFStep = 12 and batchlocation = 64 then 'Verification'
	WHEN b.WFStep = 13 and batchlocation <> 0 then 'Export'  
	END OpenBoxBatchLocation,
CASE
	WHEN M.ExportedBatchName is not null then 'Exported'
	WHEN BatchStatus = 1 Then 'Ready'
	WHEN BatchStatus = 2 Then 'In Process'
	WHEN BatchStatus = 4 Then 'Suspended'
	WHEN BatchStatus = 8 Then 'Auto-Fail'
	WHEN M.ExportedBatchName is null and b.BatchName is null then 'Pending OpenBox Import'	
	END BatchStatus, b.TotalImages,
CASE
	WHEN M.BatchClassName = 'SM' Then 'SM'
	ELSE 'P'
	END SpecialMediaoPaper
from fcsMaster M
left join tbatches b
on M.BatchName = b.BatchName
group by M.RMN, M.TrackingNo, M.BatchName, M.DCSID, b.WFStep, b.BatchLocation,
M.ExportedBatchName, b.BatchStatus, b.BatchName, b.TotalImages, M.BatchClassName) X
left join uDCSIDs u
on X.BatchName = u.pbatch
where u.UploadDateTime is null
group by X.RMN, X.TrackingNo, X.BatchName, X.DCSID, X.OpenBoxBatchLocation, X.BatchStatus, X.TotalImages,
X.SpecialMediaoPaper) T
left join ibatches i
on T.Batchname = i.BatchName
order by T.BatchName''')
finale3 = pd.DataFrame(results3)
finale3.columns = ['RMN', 'TrackingNo', 'BatchName', 'InvTime', 'TAT(Days)', 'DCSID', 'OpenBoxBatchLocation', 'BatchStatus', 'TotalImages(OpenBox)', 'SpecialMedia/Paper']


# In[18]:


writer = pd.ExcelWriter(r'\\atl-va-fs06\data\FCS\2021\FCS_TAT_' + fileDate + '.xlsx', engine='xlsxwriter')


# In[19]:


finale.to_excel(writer, sheet_name='FCS Summary', index=False)
finale2.to_excel(writer, sheet_name='Outstanding Batches', index=False)
finale3.to_excel(writer, sheet_name='Outstanding DCSIDs', index=False)

workbook = writer.book
border_fmt = workbook.add_format({'bottom':1, 'top':1, 'left':1, 'right':1})
worksheet = writer.sheets['FCS Summary']
worksheet.set_column('A:A', 17)
worksheet.set_column('B:B', 19)
worksheet.set_column('C:C', 12)
worksheet.set_column('D:D', 14)
worksheet.set_column('E:E', 14)
worksheet.set_column('F:F', 21)
worksheet.set_column('G:G', 20)
worksheet.set_column('H:H', 20)
worksheet.set_column('I:I', 20)
worksheet.set_column('J:J', 11)
worksheet.conditional_format(xlsxwriter.utility.xl_range(0,0, len(finale), len(finale.columns) - 1), {'type': 'no_errors', 'format': border_fmt})
                                                                                                  
options = {'style': 'Table Style Medium 2',
          'columns': [{'header': 'RMN'},
                     {'header': 'TrackingNo'},
                     {'header': 'ClaimType'},
                     {'header': 'TotalBatches'},
                     {'header': 'TotalDCSIDs'},
                     {'header': 'InProgress(Batches)'},
                     {'header': 'Exported(Batches)'},
                     {'header': 'Uploaded(Batches)'},
                     {'header': 'Uploaded(DCSIDs)'},
                     {'header': 'Status'}]}
worksheet.add_table(xlsxwriter.utility.xl_range(0,0, len(finale), len(finale.columns) - 1), options)

worksheet = writer.sheets['Outstanding Batches']
worksheet.set_column('A:A', 17)
worksheet.set_column('B:B', 19)
worksheet.set_column('C:C', 15)
worksheet.set_column('D:D', 10)
worksheet.set_column('E:E', 12)
worksheet.set_column('F:F', 24)
worksheet.set_column('G:G', 23)
worksheet.set_column('H:H', 24)
worksheet.set_column('I:I', 21)
worksheet.set_column('J:J', 21)
worksheet.set_column('K:K', 28)
worksheet.conditional_format(xlsxwriter.utility.xl_range(0,0, len(finale2), len(finale2.columns) - 1), {'type': 'no_errors', 'format': border_fmt})
options = {'style': 'Table Style Medium 2',
          'columns': [{'header': 'RMN'},
                     {'header': 'TrackingNo'},
                     {'header': 'BatchName'},
                     {'header': 'InvDate'},  
                     {'header': 'TAT(Days)'},
                     {'header': 'OpenBoxBatchLocation'},
                     {'header': 'BatchStatus'},                         
                     {'header': 'TotalImages(OpenBox)'},
                     {'header': 'SpecialMedia/Paper'},
                     {'header': 'Paper(DCSID Count)'},
                     {'header': 'SpecialMedia(DCSID Count)'}]}
worksheet.add_table(xlsxwriter.utility.xl_range(0,0, len(finale2), len(finale2.columns) - 1), options)

worksheet = writer.sheets['Outstanding DCSIDs']
worksheet.set_column('A:A', 17)
worksheet.set_column('B:B', 19)
worksheet.set_column('C:C', 15)
worksheet.set_column('D:D', 10)
worksheet.set_column('E:E', 12)
worksheet.set_column('F:F', 22)
worksheet.set_column('G:G', 24)
worksheet.set_column('H:H', 23)
worksheet.set_column('I:I', 24)
worksheet.set_column('J:J', 21)
worksheet.conditional_format(xlsxwriter.utility.xl_range(0,0, len(finale3), len(finale3.columns) - 1), {'type': 'no_errors', 'format': border_fmt})
options = {'style': 'Table Style Medium 2',
          'columns': [{'header': 'RMN'},
                     {'header': 'TrackingNo'},
                     {'header': 'BatchName'},
                     {'header': 'InvDate'},
                     {'header': 'TAT(Days)'},
                     {'header': 'DCSID'},                      
                     {'header': 'OpenBoxBatchLocation'},
                     {'header': 'BatchStatus'},                         
                     {'header': 'TotalImages(OpenBox)'},
                     {'header': 'SpecialMedia/Paper'}]}
worksheet.add_table(xlsxwriter.utility.xl_range(0,0, len(finale3), len(finale3.columns) - 1), options)

writer.save()


# In[20]:


#receiver = ['Clarissa.Hubbard@exelaonline.com', 'John.Blankenship@exelaonline.com', 'Trimeka.Parks@exelaonline.com', 'Matthew.Marlow@exelaonline.com', 'Donald.BenDavid@exelaonline.com', 'Rebekah.Taulbee@exelaonline.com', 'Virginia.Todd@exelaonline.com', 'Kristen.Adams@exelaonline.com', 'Donna.Leach@exelaonline.com', 'Stephanie.King@exelaonline.com', 'Lisa.Stewart@exelaonline.com', 'Brandon.Lewis@exelaonline.com', 'Tausha.Woods@exelaonline.com', 'Danny.Bishop@exelaonline.com', 'Robert.Searcy@exelaonline.com', 'Juarez.Johnson@exelaonline.com']
receiver = ['Stephanie.King@exelaonline.com', 'Danny.Bishop@exelaonline.com', 'Summer.Owens@exelaonline.com', 'John.VanWinkle@exelaonline.com']
#copy = ['lunnie.smith@exelaonline.com', 'sam.momin@exelaonline.com', 'mark.bertram@exelaonline.com', 'richard.hyde@exelaonline.com', 'sasha.wernersbach@exelaonline.com', 'John.VanWinkle@exelaonline.com']
copy = ['Summer.Owens@exelaonline.com', 'Elizabeth.Parker@exelaonline.com', 'Lisa.Stewart@exelaonline.com', 'Clarissa.Hubbard@exelaonline.com', 'Kristen.Adams@exelaonline.com', 'Stephanie.King@exelaonline.com', 'lunnie.smith@exelaonline.com', 'sam.momin@exelaonline.com', 'sasha.wernersbach@exelaonline.com']
body = 'Please find the latest FCS TAT summary within attached spreadsheet.'
xfilename = filename + '.xlsx'


# In[21]:


yag = yagmail.SMTP(user={'atlhome@lason.com': 'Exela Automated'}, password='lason123', 
                   host='smtprelay.exelaonline.com', port=25, 
                   smtp_ssl=False, smtp_starttls=False, smtp_skip_login=True)

yag.send(
    to=receiver,
    cc=copy,
    subject='FCS TAT ' + fileDate2,
    contents=body,
    attachments=xfilename
    )


# In[ ]:





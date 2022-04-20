#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyodbc
import pandas as pd
from datetime import datetime
import os
import sqlite3
from sqlalchemy import create_engine
import xlsxwriter
#import yagmail
from io import BytesIO
from os.path import basename
#import matplotlib.pyplot as plt
#import openpyxl
#from openpyxl import load_workbook
from zipfile import ZipFile
import zipfile


# In[2]:


#pip list


# In[3]:


cnxn = pyodbc.connect("Driver={SQL Server};SERVER=lon-va-sql-1\p1,2001;Database=DVA;UID=lrmo;PWD=Happy_Trails")


# In[4]:


fileDate = datetime.now().strftime("%m%d%Y_%H%M")
fileDate2 = datetime.now().strftime("%Y%m%d")
outDIR = (r'\\mtv-va-fs06\data\OMPF\2022')
filename = os.path.join(outDIR, 'OMPF_Summary_' + fileDate)


# In[5]:


sql_query03 = pd.read_sql_query('''
select distinct p.RMN, p.Pbatch, p.dcsid, pp.BatchClassName
from PbatchDCSMapping p WITH (NOLOCK)
left join PhysicalBatch pp
on p.Pbatch = pp.PBatch
where exists (select * from customerCheckIn where p.RMN = RMN
and claimtype = 'OMPF' and insertdate >= '2021-06-10') and p.PBatch like '02%'
and pp.InvTime >= '2021-06-10'
''', cnxn)


# In[6]:


sql_query = pd.read_sql_query('''select distinct Z.RMN, Z.TrackingNo, Z.BatchName,
Z.BatchClassName, Z.dcsid, Z.ScannedBatchName, Z.InvTime,
s.kbatch ExportedBatchName
FROM
(select distinct Y.RMN, Y.TrackingNo, Y.BatchName,
Y.BatchClassName, Y.dcsid, s.batchname ScannedBatchName, Y.InvTime
FROM
(select distinct X.RMN, X.TrackingNo, X.BatchName,
X.BatchClassName, p.dcsid, X.InvTime
FROM
(select distinct T.RMN, T.TrackingNo TrackingNo,
p.PBatch BatchName, p.BatchClassName, p.InvTime
FROM
(select distinct RTRIM(c.RMN) RMN, c.trackingno
from customerCheckIn c WITH (NOLOCK)
where c.trackingno is not null
and c.trackingno <> ''
and c.trackingno <> '1234DUMMY1234'
and c.claimtype = 'OMPF'
and c.insertdate >= '2021-06-10') T
left join PhysicalBatch p
on T.RMN = p.RMN
where  p.InvTime >= '2021-06-10'
and p.pbatch not like '%TEST%' 
and p.pbatch not like '%TRAIN%') X
left join PbatchDCSMapping p
on X.BatchName = p.Pbatch) Y
left join [lon-va-sql-4\p4].TURBOSCANNG1.dbo.ts_audit s
on Y.BatchName = s.batchname) Z
left join stats s
on Z.BatchName = s.kbatch''', cnxn)


# In[7]:


sql_query04 = pd.read_sql_query('''
select distinct X.BatchName,
X.WFStep, X.BatchLocation,
X.BatchStatus, X.TotalImages,
X.InvTime
FROM
(select distinct T.BatchName, T.WFStep, T.BatchLocation, 
T.BatchStatus, T.TotalImages, max(p.InvTime) InvTime
FROM
(select distinct BatchName, WFStep, BatchLocation, BatchStatus, TotalImages 
from [lon-va-sql-4\p4].turboscanng1.dbo.batches b) T
left join PhysicalBatch p
on T.BatchName = p.PBatch
where exists (select * from customerCheckIn where p.RMN = RMN
and claimtype = 'OMPF' and insertdate >= '2021-06-10')
--and p.InvTime >= '2021-06-10' 
and T.BatchName not like '%TEST%'
and T.BatchName not like '%TRAIN%'
group by T.BatchName, T.WFStep, T.BatchLocation, 
T.BatchStatus, T.TotalImages) X
where X.InvTime >= '2021-06-10'
''', cnxn)


# In[8]:


sql_query05 = pd.read_sql_query('''
select distinct BatchName 
from [lon-va-sql-2\p2].IBMLTEST_Data.dbo.batchtable''', cnxn)


# In[9]:


sql_query02 = pd.read_sql_query('''
select distinct d.RMN, d.Pbatch, d.dcsid, d.ImageID,
CONVERT(nvarchar, max(d.ftpstime), 101) UploadDateTime
--into xoDCSIDsExported
from document d
where exists (select * from customerCheckIn where d.RMN = RMN
and claimtype = 'OMPF' and insertdate >= '2021-06-10')
and d.ImageDateTime >= '2021-06-10'
--and exists (select * from document where d.dcsID = dcsid
--and d.PBatch = pbatch and ftpstime is not null)
group by d.RMN, d.Pbatch, d.dcsid, d.ImageID
order by UploadDateTime''', cnxn)


# In[10]:


sql_query06 = pd.read_sql_query('''
select distinct BatchName, TotalImages from turboscanng_ocr1.dbo.Batches
where BatchLocation <> 0
union
select distinct BatchName, TotalImages from turboscanng_ocr2.dbo.Batches
where BatchLocation <> 0
union
select distinct BatchName, TotalImages from turboscanng_ocr3.dbo.Batches
where BatchLocation <> 0
union
select distinct BatchName, TotalImages from turboscanng_ocr4.dbo.Batches
where BatchLocation <> 0
union
select distinct BatchName, TotalImages from turboscanng_ocr5.dbo.Batches
where BatchLocation <> 0''',cnxn)


# In[11]:


engine = create_engine('sqlite://', echo=False)


# In[12]:


df = pd.DataFrame(sql_query)
df02 = pd.DataFrame(sql_query02)
df03 = pd.DataFrame(sql_query03)
df04 = pd.DataFrame(sql_query04)
df05 = pd.DataFrame(sql_query05)
df06 = pd.DataFrame(sql_query06)


# In[13]:


df.to_sql('ompfMaster', engine, if_exists='replace', index=False)
df02.to_sql('uDCSIDs', engine, if_exists='replace', index=False)
df03.to_sql('aDCSIDs', engine, if_exists='replace', index=False)
df04.to_sql('tbatches', engine, if_exists='replace', index=False)
df05.to_sql('ibatches', engine, if_exists='replace', index=False)
df06.to_sql('twobatches', engine, if_exists='replace', index=False)

results = engine.execute('''select distinct X.RMN, X.TrackingNo,
X.Batches, X.DCSIDs,
X.Batches - X.ExportedBatches,
X.ExportedBatches, X.UploadedBatches,
X.UploadedDCSIDs
FROM
(select distinct T.RMN, T.TrackingNo,
count(distinct T.BatchName) Batches,
count(distinct T.DCSID) DCSIDs,
count(distinct T.ExportedBatchName) ExportedBatches,
count(distinct T.UploadedBatch) UploadedBatches,
count(distinct T.UploadedDCSID) UploadedDCSIDs
FROM
(select distinct M.RMN, M.TrackingNo,
M.BatchName, M.DCSID, M.ExportedBatchName, 
u.pbatch UploadedBatch, u.dcsid UploadedDCSID
from ompfMaster M
left join uDCSIDs u
on M.BatchName = u.PBatch) T
group by T.RMN, T.TrackingNo) X
where (X.Batches <> X.UploadedBatches and X.DCSIDs <> X.UploadedDCSIDs and X.Batches <> X.UploadedBatches)''')
finale = pd.DataFrame(results)
finale.columns = ['RMN', 'TrackingNo', 'TotalBatches', 'TotalDCSIDs', 'InProgress(Batches)', 'Exported(Batches)', 'Uploaded(Batches)', 'Uploaded(DCSIDs)']
#finale


# In[14]:


results2 = engine.execute('''select distinct T.RMN, T.TrackingNo, T.BatchName,
strftime('%Y-%m-%d',T.InvTime) InvTime,
CASE
    When T.OpenBoxBatchLocation is null Then '-'
    Else T.OpenBoxBatchLocation
    END OpenBoxBatchLocation,
CASE
    When two.BatchName is not null Then 'WIP 2nd Job OCR'
	When T.BatchStatus = 'Exported' and two.BatchName is null Then 'Pending Upload'
	When T.BatchStatus = 'Pending OpenBox Import' and i.BatchName is null Then 'Pending Scan'
	Else T.BatchStatus
	END BatchStatus, 
CASE
    WHEN two.BatchName is not null Then two.TotalImages
    Else T.TotalImages
    END TotalImages,
    T.SpecialMediaoPaper, T.DCSIDPaper, T.DCSIDSM
FROM
(select X.RMN, X.TrackingNo, X.BatchName, X.InvTime,
X.OpenBoxBatchLocation, X.BatchStatus,
X.TotalImages, X.SpecialMediaoPaper, X.DCSIDPaper, X.DCSIDSM,
count(distinct u.imageID) TotalDocs,
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
from ompfMaster M
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
left join twobatches two
on T.Batchname = two.BatchName
order by T.BatchName''')
finale2 = pd.DataFrame(results2)
#finale2
#commented out cause results is null
#finale3.columns = ['RMN', 'TrackingNo', 'BatchName', 'DCSID', 'OpenBoxBatchLocation', 'BatchStatus', 'TotalImages(OpenBox)', 'SpecialMedia/Paper']
#finale2


# In[15]:


if finale2.empty:
    results2 = {'RMN': [''], 
                'TrackingNo': [''], 
                'BatchName': [''],
                'InvDate': [''],
                'OpenBoxBatchLocation': [''], 
                'BatchStatus': [''], 
                'TotalImages(OpenBox)': [''], 
                'SpecialMedia/Paper': [''], 
                'Paper(DCSID Count)': [''], 
                'SpecialMedia(DCSID Count)': ['']}
    finale2 = pd.DataFrame(results2)
    finale2.columns = ['RMN', 'TrackingNo', 'BatchName', 'InvDate', 'OpenBoxBatchLocation', 'BatchStatus', 'TotalImages(OpenBox)', 'SpecialMedia/Paper', 'Paper(DCSID Count)', 'SpecialMedia(DCSID Count)']
    
else:
    finale2.columns = ['RMN', 'TrackingNo', 'BatchName', 'InvDate', 'OpenBoxBatchLocation', 'BatchStatus', 'TotalImages(OpenBox)', 'SpecialMedia/Paper', 'Paper(DCSID Count)', 'SpecialMedia(DCSID Count)']


# In[16]:


results3 = engine.execute('''select distinct T.RMN, T.TrackingNo, T.BatchName, 
strftime('%Y-%m-%d',T.InvTime) InvTime,
xx.DCSID,
T.OpenBoxBatchLocation,
CASE
    When two.BatchName is not null Then 'WIP 2nd Job OCR'
	When T.BatchStatus = 'Exported' and two.BatchName is null Then 'Pending Upload'
	When T.BatchStatus = 'Pending OpenBox Import' and i.BatchName is null Then 'Pending Scan'
	Else T.BatchStatus
	END BatchStatus, 
CASE
    WHEN two.BatchName is not null Then two.TotalImages
    Else T.TotalImages
    END TotalImages,
    T.SpecialMediaoPaper
FROM
(select X.RMN, X.TrackingNo, X.BatchName, X.InvTime, X.dcsid DCSID,
X.OpenBoxBatchLocation, X.BatchStatus,
X.TotalImages, X.SpecialMediaoPaper,
count(distinct u.imageID) TotalDocs,
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
from ompfMaster M
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
left join twobatches two
on T.Batchname = two.BatchName
left join uDCSIDs xx
on T.DCSID = xx.DCSID
order by T.BatchName''')
finale3 = pd.DataFrame(results3)
#finale3
#commented out cause results is null
#finale3.columns = ['RMN', 'TrackingNo', 'BatchName', 'DCSID', 'OpenBoxBatchLocation', 'BatchStatus', 'TotalImages(OpenBox)', 'SpecialMedia/Paper']


# In[17]:


if finale3.empty:
    results3 = {'RMN': [''], 
                'TrackingNo': [''], 
                'BatchName': [''], 
                'InvDate': [''],
                'DCSID': [''],
                'OpenBoxBatchLocation': [''], 
                'BatchStatus': [''], 
                'TotalImages(OpenBox)': [''], 
                'SpecialMedia/Paper': ['']}
    finale3 = pd.DataFrame(results3)
    finale3.columns = ['RMN', 'TrackingNo', 'BatchName', 'InvDate', 'DCSID', 'OpenBoxBatchLocation', 'BatchStatus', 'TotalImages(OpenBox)', 'SpecialMedia/Paper']
    
else: finale3.columns = ['RMN', 'TrackingNo', 'BatchName', 'InvDate', 'DCSID', 'OpenBoxBatchLocation', 'BatchStatus', 'TotalImages(OpenBox)', 'SpecialMedia/Paper']


# In[18]:


writer = pd.ExcelWriter(r'\\mtv-va-fs06\data\OMPF\2022\OMPF_Summary_' + fileDate + '-LONDON.xlsx', engine='xlsxwriter')


# In[19]:


finale5 = finale2.copy()


# In[20]:


df_pt = finale5[['RMN', 'TrackingNo', 'BatchName', 'InvDate', 'OpenBoxBatchLocation', 'BatchStatus', 'TotalImages(OpenBox)', 'SpecialMedia/Paper', 'Paper(DCSID Count)', 'SpecialMedia(DCSID Count)']]


# In[21]:


##df_pt=pd.pivot_table(finale2, index = ['RMN'], values= ['BatchName'], columns='InvDate', aggfunc='count')
#df_pt=pd.pivot_table(finale2, index = ['InvDate'], values= ['BatchName'], aggfunc='count')
#df_pt=pd.pivot_table(data=finale2,index=['RMN', 'BatchName', 'InvDate', 'OpenBoxBatchLocation', 'BatchStatus'])
#df_pt=pd.pivot_table(data=finale2,index=['RMN', 'BatchName', 'InvDate'], fill_value=0)
#df_pt=pd.pivot_table(data=finale2,index=['RMN', 'TrackingNo', 'BatchName', 'InvDate', 'OpenBoxBatchLocation', 'BatchStatus'], fill_value='-')
df_pt=pd.pivot_table(data=finale2,index=['RMN', 'TrackingNo', 'BatchName', 'InvDate', 'OpenBoxBatchLocation', 'BatchStatus'], fill_value='-')


# In[22]:


#df_pt['InvDate'] = pd.to_datetime(df_pt['InvDate'])
df_pt.sort_values(by=['InvDate'], inplace=True, ascending=True)
#df_pt


# In[23]:


finale.to_excel(writer, sheet_name='OMPF Summary', index=False)
finale2.to_excel(writer, sheet_name='Outstanding Batches', index=False)
finale3.to_excel(writer, sheet_name='Outstanding DCSIDs', index=False)
#finale4.to_excel(writer, sheet_name='RMN Batch View', index=False)

#test below
df_pt.to_excel(writer, sheet_name='RMN TAT Summary')
#

workbook = writer.book
border_fmt = workbook.add_format({'bottom':1, 'top':1, 'left':1, 'right':1})
worksheet = writer.sheets['OMPF Summary']
worksheet.set_column('A:A', 17)
worksheet.set_column('B:B', 19)
worksheet.set_column('C:C', 13)
worksheet.set_column('D:D', 13)
worksheet.set_column('E:E', 19)
worksheet.set_column('F:F', 17)
worksheet.set_column('G:G', 18)
worksheet.set_column('H:H', 17)
worksheet.conditional_format(xlsxwriter.utility.xl_range(0,0, len(finale), len(finale.columns) - 1), {'type': 'no_errors', 'format': border_fmt})
                                                                                                  
options = {'style': 'Table Style Medium 2',
          'columns': [{'header': 'RMN'},
                     {'header': 'TrackingNo'},
                     {'header': 'TotalBatches'},
                     {'header': 'TotalDCSIDs'},
                     {'header': 'InProgress(Batches)'},
                     {'header': 'Exported(Batches)'},
                     {'header': 'Uploaded(Batches)'},
                     {'header': 'Uploaded(DCSIDs)'}]}
worksheet.add_table(xlsxwriter.utility.xl_range(0,0, len(finale), len(finale.columns) - 1), options)

worksheet = writer.sheets['Outstanding Batches']
worksheet.set_column('A:A', 17)
worksheet.set_column('B:B', 19)
worksheet.set_column('C:C', 15)
worksheet.set_column('D:D', 10)
worksheet.set_column('E:E', 23)
worksheet.set_column('F:F', 23)
worksheet.set_column('G:G', 21)
worksheet.set_column('H:H', 19)
worksheet.set_column('I:I', 26)
worksheet.set_column('J:J', 27)
worksheet.conditional_format(xlsxwriter.utility.xl_range(0,0, len(finale2), len(finale2.columns) - 1), {'type': 'no_errors', 'format': border_fmt})
options = {'style': 'Table Style Medium 2',
          'columns': [{'header': 'RMN'},
                     {'header': 'TrackingNo'},
                     {'header': 'BatchName'}, 
                     {'header': 'InvDate'},                          
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
worksheet.set_column('E:E', 22)
worksheet.set_column('F:F', 23)
worksheet.set_column('G:G', 23)
worksheet.set_column('H:H', 21)
worksheet.set_column('I:I', 19)
worksheet.conditional_format(xlsxwriter.utility.xl_range(0,0, len(finale3), len(finale3.columns) - 1), {'type': 'no_errors', 'format': border_fmt})
options = {'style': 'Table Style Medium 2',
          'columns': [{'header': 'RMN'},
                     {'header': 'TrackingNo'},
                     {'header': 'BatchName'},
                     {'header': 'InvDate'},                      
                     {'header': 'DCSID'},                      
                     {'header': 'OpenBoxBatchLocation'},
                     {'header': 'BatchStatus'},                         
                     {'header': 'TotalImages(OpenBox)'},
                     {'header': 'SpecialMedia/Paper'}]}
worksheet.add_table(xlsxwriter.utility.xl_range(0,0, len(finale3), len(finale3.columns) - 1), options)



worksheet = writer.sheets['RMN TAT Summary']
worksheet.set_column('A:A', 18)
worksheet.set_column('B:B', 15)
worksheet.set_column('C:C', 15)
worksheet.set_column('D:D', 19)
worksheet.set_column('E:E', 26)
worksheet.set_column('F:F', 21)
worksheet.set_column('G:G', 19)
worksheet.set_column('H:H', 26)
worksheet.set_column('I:I', 21)
#worksheet.conditional_format(xlsxwriter.utility.xl_range(0,0, len(df_pt), len(df_pt.columns) + 2), {'type': 'no_errors', 'format': border_fmt})
#options = {'style': 'Table Style Medium 3',
#          'columns': [{'header': 'RMN'},
#                     {'header': 'BatchName'},                      
#                     {'header': 'InvDate'},                      
#                     {'header': 'Paper(DCSID Count)'},
#                     {'header': 'SpecialMedia(DCSID Count)'},                         
#                     {'header': 'TotalImages(OpenBox)'}]}
#worksheet.add_table(xlsxwriter.utility.xl_range(0,0, len(df_pt), len(df_pt.columns) + 2), options)



writer.save()


# In[24]:


xfilename = filename + '-LONDON.xlsx'
#read_file = pd.read_excel(xfilename)
#xfilename2 = read_file.to_csv(filename + '.csv', index=None, header=True)


# In[25]:


with ZipFile(filename + '-LONDON.zip', 'w', zipfile.ZIP_DEFLATED) as zipObj:
    zipObj.write(xfilename, basename(xfilename))


# In[ ]:





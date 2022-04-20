#!/usr/bin/env python
# coding: utf-8

# In[11]:


import pyodbc
import pandas as pd
from datetime import datetime
import os
import sqlite3
from sqlalchemy import create_engine
import xlsxwriter
import yagmail


# In[12]:


cnxn = pyodbc.connect("Driver={SQL Server};SERVER=mtv-va-sql-4\p1;Database=TURBOSCANNG1;UID=dva;PWD=Happy_Trails")


# In[13]:


fileDate = datetime.now().strftime("%m%d%Y_%H%M")
fileDate2 = datetime.now().strftime("%Y%m%d")
outDIR = (r'\\atl-va-fs06\data\Verification\2021')
filename = os.path.join(outDIR, 'Verification_' + fileDate)


# In[14]:


sql_query01 = pd.read_sql_query('''
SELECT distinct P.RMN, T.BatchName, T.TotalImages,
case 
WHEN WFStep = 1 and BatchLocation = '1' then 'Scan/Capture'
WHEN WFStep = 2 and BatchLocation = '2' then 'Enhance1'
WHEN WFStep = 3 and BatchLocation = '16' then 'FOCR1'
WHEN WFStep = 4 and BatchLocation = '2' then 'Enhance2'
WHEN WFStep = 5 and BatchLocation = '4' then 'Separation'
WHEN WFStep = 6 and BatchLocation = '128' then 'QA1'
WHEN WFStep = 7 and BatchLocation = '8' then 'AutoIndex'
WHEN WFStep = 8 and BatchLocation = '128' then 'QA2'
WHEN WFStep = 9 and BatchLocation = '128' then 'QA3'
WHEN WFStep = 10 and BatchLocation = '32' then 'ManualIndex'
WHEN WFStep = 11 and BatchLocation = '32' then 'ManualIndex2'
WHEN WFStep = 12 and BatchLocation = '64' then 'Verification/BVTI'
WHEN WFStep = 13 and BatchLocation = '256' then 'Export'
WHEN WFStep = 13 and BatchLocation = 0 then 'Clean'
ELSE 'WFStep'+CONVERT(varchar(10), WFStep)+' BchLoc '+CONVERT(varchar(10), BatchLocation)+' - LocationError'
END as TSModule, 
case 
WHEN BatchStatus = 0 then 'Error'
WHEN BatchStatus = 1 then 'Ready'
WHEN BatchStatus = 2 then 'In Process'
WHEN BatchStatus = 4 then 'Suspended'
WHEN BatchStatus = 8 then 'Auto-Fail'
END as Status,
C.boxSource
FROM
[MTV-VA-SQL-4\P1].TURBOSCANNG1.dbo.Batches T
join [MTV-VA-SQL-1\P922].DVA.dbo.PhysicalBatch P
on T.BatchName = P.PBatch
join [MTV-VA-SQL-1\P922].DVA.dbo.customerCheckIn C
on P.RMN = C.RMN
WHERE WFStep = 12 and BatchLocation = 64
''', cnxn)


# In[15]:


finale3 = pd.DataFrame(sql_query01)


# In[16]:


if finale3.empty:
    results3 = {'RMN': [''], 
                'BatchName': [''], 
                'TotalImages(OpenBox)': [''], 
                'TSModule': [''], 
                'Status': [''], 
                'boxSource': ['']}
    finale3 = pd.DataFrame(results3)
    finale3.columns = ['RMN', 'BatchName', 'TotalImages(OpenBox)', 'TSModule', 'Status', 'boxSource']
    
else:
    finale3.columns = ['RMN', 'BatchName', 'TotalImages(OpenBox)', 'TSModule', 'Status', 'boxSource']


# In[17]:


writer = pd.ExcelWriter(r'\\atl-va-fs06\data\Verification\2021\Verification_' + fileDate + '.xlsx', engine='xlsxwriter')


# In[18]:


finale3.to_excel(writer, sheet_name='Verification Queue', index=False)

workbook = writer.book
border_fmt = workbook.add_format({'bottom':1, 'top':1, 'left':1, 'right':1})
worksheet = writer.sheets['Verification Queue']
worksheet.set_column('A:A', 18)
worksheet.set_column('B:B', 15)
worksheet.set_column('C:C', 24)
worksheet.set_column('D:D', 16)
worksheet.set_column('E:E', 11)
worksheet.set_column('F:F', 12)
worksheet.conditional_format(xlsxwriter.utility.xl_range(0,0, len(finale3), len(finale3.columns) - 1), {'type': 'no_errors', 'format': border_fmt})
                                                                                                  
options = {'style': 'Table Style Medium 2',
          'columns': [{'header': 'RMN'},
                     {'header': 'BatchName'},
                     {'header': 'TotalImages(OpenBox)'},
                     {'header': 'TSModule'},
                     {'header': 'Status'},
                     {'header': 'boxSource'}]}
worksheet.add_table(xlsxwriter.utility.xl_range(0,0, len(finale3), len(finale3.columns) - 1), options)

writer.save()


# In[19]:


#receiver = ['Clarissa.Hubbard@exelaonline.com', 'John.Blankenship@exelaonline.com', 'Trimeka.Parks@exelaonline.com', 'Matthew.Marlow@exelaonline.com', 'Donald.BenDavid@exelaonline.com', 'Rebekah.Taulbee@exelaonline.com', 'Virginia.Todd@exelaonline.com', 'Kristen.Adams@exelaonline.com', 'Donna.Leach@exelaonline.com', 'Stephanie.King@exelaonline.com', 'Lisa.Stewart@exelaonline.com', 'Brandon.Lewis@exelaonline.com', 'Tausha.Woods@exelaonline.com', 'Danny.Bishop@exelaonline.com', 'Robert.Searcy@exelaonline.com', 'Sherry.Hyde@exelaonline.com', 'Juarez.Johnson@exelaonline.com', 'Geoff.Brinton@exelaonline.com', 'Michael.Cincinelli@exelaonline.com', 'Jailyn.Allen@exelaonline.com', 'Kanzas.Hicks@exelaonline.com', 'Regina.Brady@exelaonline.com', 'Kellie.Lake@exelaonline.com', 'Summer.Owens@exelaonline.com', 'Deborah.Otis@exelaonline.com', 'Matthew.Marlow@exelaonline.com', 'Elizabeth.England@exelaonline.com', 'ryan.oquinn@exelaonline.com' ]
#copy = ['lunnie.smith@exelaonline.com', 'sam.momin@exelaonline.com', 'mark.bertram@exelaonline.com', 'richard.hyde@exelaonline.com', 'sasha.wernersbach@exelaonline.com']
receiver = ['Virginia.Brantley@exelaonline.com', 'John.Blankenship@exelaonline.com', 'Brenda.Brock@exelaonline.com', 'Chris.Birkeland@exelaonline.com' ]
copy = ['Sam.Momin@exelaonline.com', 'lunnie.smith@exelaonline.com']
body = 'Please find the latest Verification queue summary within attached spreadsheet.'
xfilename = filename + '.xlsx'


# In[20]:


yag = yagmail.SMTP(user={'atlhome@lason.com': 'Exela Automated'}, password='lason123', 
                   host='smtprelay.exelaonline.com', port=25, 
                   smtp_ssl=False, smtp_starttls=False, smtp_skip_login=True)

yag.send(
    to=receiver,
    cc=copy,
    subject='BVTI Queue ' + fileDate2,
    contents=body,
    attachments=xfilename
    )


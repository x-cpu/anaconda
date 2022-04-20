import pyodbc
import pandas as pd
import os
from datetime import datetime
#from openpyxl import load_workbook
#import xlsxwriter
#from shutil import copyfile


cnxn = pyodbc.connect("Driver={SQL Server};SERVER=mtv-va-sql-1\p922;Database=DVA;UID=webportal;PWD=Cla$$room")

fileDate = datetime.now().strftime("%m%d%Y_%H%M.xlsx")
outDIR = (r'\\atl-va-fs06\data\OMPF')
filename = os.path.join(outDIR, 'OMPF_Summary_' + fileDate)

sql_query01 = pd.read_sql_query('''select distinct RTRIM(Z.RMN) RMN, Z.TrackingNo, ISNULL(sum(Batches), 0) Batches,
                        ISNULL(sum(Z.Batches), 0) - ISNULL(sum(Z.Exported), 0) InProgress,
                        ISNULL(sum(Z.Exported), 0) Exported, Z.Uploaded
                        FROM
                        (select distinct Y.RMN, Y.TrackingNo, count(distinct Y.BatchName) Batches,
                        count(distinct Y.kbatch) Exported, count(distinct Y.uploadBatchName) Uploaded
                        FROM
                        (select distinct X.RMN, X.TrackingNo, X.BatchName, X.kbatch,
                        CASE
	                        When d.ftpstime is not null then d.PBatch
	                        END uploadBatchName
                        FROM
                        (select distinct T.RMN, T.TrackingNo, T.BatchName,
                        s.kbatch
                        FROM
                        (select distinct c.RMN, c.trackingno TrackingNo, 
                        p.PBatch BatchName
                        from customerCheckIn c 
                        left join PhysicalBatch p
                        on c.RMN = p.RMN
                        where c.claimtype = 'ompf'
                        and p.PBatch like '02%'
                        and p.InvTime >= '2020-06-01'
                        and c.trackingno is not null
                        and c.trackingno <> ''
                        and c.trackingno <> '1234DUMMY1234'
                        group by c.RMN, c.trackingno, p.PBatch) T
                        left join stats s
                        on T.BatchName = s.kbatch) X
                        left join document d
                        on X.BatchName = d.PBatch) Y
                        group by Y.RMN, Y.TrackingNo) Z
                        group by Z.RMN, Z.TrackingNo, Z.Uploaded''', cnxn)

sql_query02 = pd.read_sql_query('''select distinct RTRIM(T.RMN) RMN, T.TrackingNo, T.BatchName, 
                        T.OpenBoxBatchLocation, 
                        CASE
	                        When T.BatchStatus = 'Exported' Then 'Exported [' + CAST((T.TotalDocs - T.TotalUploaded) as varchar(50)) + 
	                        ' of ' + CAST((T.TotalDocs) as varchar(100)) + ' doc(s) pending upload]'
	                        When T.BatchStatus = 'Pending OpenBox Import' and i.BatchName is null Then 'Pending IBML Scan'
	                        Else T.BatchStatus
	                        END BatchStatus,
                        T.TotalImages
                        FROM
                        (select distinct X.RMN, X.TrackingNo, X.BatchName,
                        X.OpenBoxBatchLocation, X.BatchStatus,
                        X.TotalImages,
                        count(distinct d.imageID) TotalDocs, 
                        count(distinct d.ftpstime) TotalUploaded
                        FROM
                        (select distinct c.RMN, c.trackingno TrackingNo, p.PBatch BatchName,
                        case 
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
                        END
                        AS 'OpenBoxBatchLocation',
                        case
	                        WHEN s.kbatch is not null then 'Exported'
	                        WHEN BatchStatus = 1 Then 'Ready'
	                        WHEN BatchStatus = 2 Then 'In Process'
	                        WHEN BatchStatus = 4 Then 'Suspended'
	                        WHEN BatchStatus = 8 Then 'Auto-Fail'
	                        WHEN s.kbatch is null and b.BatchName is null then 'Pending OpenBox Import'
                        END
                        AS 'BatchStatus', b.TotalImages
                        from customerCheckIn c 
                        left join PhysicalBatch p
                        on c.RMN = p.RMN
                        left join stats s
                        on p.PBatch = s.kbatch
                        left join [mtv-va-sql-4\p1].turboscanng1.dbo.batches b
                        on p.pbatch = b.batchname
                        where c.claimtype = 'ompf'
                        and p.PBatch like '02%'
                        and p.InvTime >= '2020-06-01'
                        and c.trackingno is not null
                        and c.trackingno <> ''
                        and c.trackingno <> '1234DUMMY1234'
                        group by c.RMN, c.trackingno, p.PBatch, b.WFStep, b.BatchLocation, 
                        b.BatchStatus, s.kbatch, b.batchname, b.TotalImages) X
                        left join document d
                        on X.BatchName = d.PBatch
                        where d.ftpstime is null
                        group by X.RMN, X.TrackingNo, X.BatchName, X.BatchStatus, 
                        X.OpenBoxBatchLocation, X.BatchStatus, X.TotalImages) T
                        left join [mtv-va-sql-2\p923].IBMLTEST_Data.dbo.batchtable i
                        on T.BatchName = i.BatchName
                        order by RMN, T.TrackingNo, T.BatchName''', cnxn)

df1 = pd.DataFrame(sql_query01)
df2 = pd.DataFrame(sql_query02)

writer = pd.ExcelWriter(filename, engine='xlsxwriter')

df1.to_excel(writer, sheet_name='OMPF Summary', index = False)
df2.to_excel(writer, sheet_name='Outstanding Batches', index = False)

for column in df1:
    column_length = max(df1[column].astype(str).map(len).max(), len(column))
    col_idx = df1.columns.get_loc(column)
    writer.sheets['OMPF Summary'].set_column(col_idx, col_idx, column_length + 1)

for column in df2:
    column_length = max(df2[column].astype(str).map(len).max(), len(column))
    col_idx = df2.columns.get_loc(column)
    writer.sheets['Outstanding Batches'].set_column(col_idx, col_idx, column_length + 1)

writer.save()

#df = pd.read_excel(filename, sheet_name='OMPF Summary')
#df['almost_done'] = df['InProgress'].astype(float)
#almost_done = 1
#df.style.apply(lambda x: ['background:lightgreen' if x == almost_done else 'background:red' for x in df.almost_done], axis =0 )


#df.to_excel('styled.xlsx', engine='openpyxl', index = False)

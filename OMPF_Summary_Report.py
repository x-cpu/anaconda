import pyodbc
import pandas as pd
#import workbook

server = 'mtv-va-sql-1\p922'
database = 'DVA'
username = 'dva'
password = 'Happy_Trails'

cnxn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER=mtv-va-sql-4\p1;DATABASE=TURBOSCANNG1;UID=dva;PWD=Happy_Trails")

cursor = cnxn.cursor()

SQLCommand = ("select batchname from batches where batchname = '02203001701601'")

cursor.execute(SQLCommand).fetchall()
df = pd.read_sql_query(SQLCommand, cnxn)

writer = pd.ExcelWriter('c:\temp\foo.xlsx')
df.to_excel(writer, sheet_name='bar')
writer.save()

#pd.read_sql('select batchname from batches where batchname = '''02203001701601'''',cnxn).to_excel('c:\temp\foo.xlsx')
#pd.read_sql(SQLCommand,cnxn).to_excel('c:\temp\foo.xlsx')

#for row in cursor:
#        print('row = %r' % (row,))

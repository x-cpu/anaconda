{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "alone-journey",
   "metadata": {},
   "outputs": [],
   "source": [
    "import pyodbc\n",
    "import pandas as pd\n",
    "from datetime import datetime\n",
    "import os\n",
    "import sqlite3\n",
    "from sqlalchemy import create_engine\n",
    "import csv\n",
    "from zipfile import ZipFile\n",
    "import mF\n",
    "from os.path import basename\n",
    "import gobble \n",
    "import yagmail\n",
    "import zipfile"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "general-athletics",
   "metadata": {},
   "outputs": [],
   "source": [
    "cnxn = pyodbc.connect(\"Driver={SQL Server};SERVER=mtv-va-sql-1\\p922;Database=DVA;UID=webportal;PWD=Cla$$room\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "cordless-composition",
   "metadata": {},
   "outputs": [],
   "source": [
    "#fileDate = datetime.now().strftime(\"%m%d%Y_%H%M.csv\")\n",
    "#fileDate = datetime.now().strftime(\"%Y%m%d%H%M%S.csv\")\n",
    "fileDate = datetime.now().strftime(\"%Y%m%d%H%M%S\")\n",
    "outDIR = (r'\\\\atl-va-fs06\\data\\Daily\\BVTI Image Review')\n",
    "filename = os.path.join(outDIR, 'BVTIImageReview-' + fileDate)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "specific-bracket",
   "metadata": {},
   "outputs": [],
   "source": [
    "#sql_query01 = pd.read_sql_query('''select distinct\n",
    "#d.RMN,\n",
    "#d.dcsID,\n",
    "#d.ImageID,\n",
    "#CONVERT(varchar, d.ImageDateTime, 23) 'PDF Created',\n",
    "#d.NumPages 'Image Count Per PDF',\n",
    "#c.Firstname xFirstname, c.Lastname xLastname,\n",
    "#d.FileNumber xFileNumber, d.docidDoctype DocType,\n",
    "#c.Recvdate 'Date of Receipt',\n",
    "#cc.claimtype\n",
    "#from document d\n",
    "#left join CustomerDATA c\n",
    "#on d.dcsID = c.dcsID and d.Pbatch = c.Pbatch\n",
    "#left join customerCheckIn cc\n",
    "#on d.RMN = cc.RMN\n",
    "#where d.ftpstime >= CONVERT(varchar, GETDATE(), 23)''', cnxn)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "incorrect-wednesday",
   "metadata": {},
   "outputs": [],
   "source": [
    "sql_query01 = pd.read_sql_query('''select distinct\n",
    "d.RMN,\n",
    "d.dcsID,\n",
    "d.ImageID,\n",
    "CONVERT(varchar, d.ImageDateTime, 23) 'PDF Created',\n",
    "d.NumPages 'Image Count Per PDF',\n",
    "c.Firstname xFirstname, c.Lastname xLastname,\n",
    "d.FileNumber xFileNumber, d.docidDoctype DocType,\n",
    "c.Recvdate 'Date of Receipt',\n",
    "cc.claimtype\n",
    "from document d\n",
    "left join CustomerDATA c\n",
    "on d.dcsID = c.dcsID and d.Pbatch = c.Pbatch\n",
    "left join customerCheckIn cc\n",
    "on d.RMN = cc.RMN\n",
    "where d.ftpstime >= dateadd(day,datediff(day,1,GETDATE()),0)\n",
    "and d.ftpstime < dateadd(day,datediff(day,0,GETDATE()),0)\n",
    "''', cnxn)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "competent-report",
   "metadata": {},
   "outputs": [],
   "source": [
    "df = pd.DataFrame(sql_query01)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "provincial-minnesota",
   "metadata": {},
   "outputs": [],
   "source": [
    "df.columns = ['RMN', 'dcsID', 'ImageID', 'PDF Created', 'Image Count Per PDF',\n",
    "       'xFirstname', 'xLastname', 'xFileNumber', 'DocType', 'Date of Receipt',\n",
    "       'claimtype']"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "stock-doubt",
   "metadata": {},
   "outputs": [],
   "source": [
    "df['Veteran Name'] = df['xFirstname'].str[:1] + '*** ' + df['xLastname'].str[:1] + '***'"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "compliant-combination",
   "metadata": {},
   "outputs": [],
   "source": [
    "df['FileNumber'] = '********' + df['xFileNumber'].str.strip().str[-1]"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "alone-mirror",
   "metadata": {},
   "outputs": [],
   "source": [
    "dfx01 = df.loc[:, ['RMN', 'dcsID', 'ImageID', 'PDF Created', 'Image Count Per PDF',\n",
    "       'Veteran Name', 'FileNumber', 'DocType', 'Date of Receipt',\n",
    "       'claimtype', 'xFirstname', 'xLastname', 'xFileNumber']]"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "rough-journalist",
   "metadata": {},
   "outputs": [],
   "source": [
    "dfx02 = dfx01.drop(['xFirstname', 'xLastname', 'xFileNumber'], axis=1)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "informed-atlantic",
   "metadata": {},
   "outputs": [],
   "source": [
    "dfx02.to_csv(filename + '.csv', index = False)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "magnetic-alexander",
   "metadata": {},
   "outputs": [],
   "source": [
    "with ZipFile(filename + '.zip', 'w', zipfile.ZIP_DEFLATED) as zipObj:\n",
    "    zipObj.write(filename + '.csv', basename(filename + '.csv'))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "answering-implementation",
   "metadata": {},
   "outputs": [],
   "source": [
    "fileDate2 = datetime.now().strftime(\"%Y/%m/%d %H:%M:%S\")\n",
    "body = 'Please find the latest file within attached zip.'"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "brilliant-serve",
   "metadata": {},
   "outputs": [],
   "source": [
    "yag = yagmail.SMTP(user={'atlhome@lason.com': 'Exela Automated'}, password='lason123', \n",
    "                   host='smtprelay.exelaonline.com', port=25, \n",
    "                   smtp_ssl=False, smtp_starttls=False, smtp_skip_login=True)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "naval-complement",
   "metadata": {},
   "outputs": [],
   "source": [
    "yag.send(\n",
    "    to=mF.ENormaStits(),\n",
    "    #to=gobble.deesKnuts(),\n",
    "    cc=gobble.deesKnuts(),\n",
    "    subject='BVTI Image Review ' + fileDate2,\n",
    "    contents=body,\n",
    "    attachments=filename + '.zip'\n",
    "    )"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "spectacular-destination",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.7.9"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}

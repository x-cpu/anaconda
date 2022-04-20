import pandas as pd
import numpy as np
from xlsxwriter.utility import xl_rowcol_to_cell
from pathlib import Path

excelFile = Path(r'K:\OMPF\OMPF_Summary_01272021_1611.xlsx')

df = pd.read_excel(excelFile)
df.head()

"""
Simple script to convert a xlsx (Excel) file to a csv file
"""


import pandas as pd


xls_file_path = "sensors/FluksoTechnical.xlsx"
csv_file_path = "sensors/FluksoTechnical_Flukso.csv"

read_file = pd.read_excel(xls_file_path, sheet_name="Flukso")
print(read_file.head(10))
read_file.to_csv(csv_file_path, index=None, header=True)
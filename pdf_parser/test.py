#TODO Add a another clause where it will only append if the nam

import pdfplumber
import pandas as pd

pdf = pdfplumber.open('test_doc.pdf')

total_pages = len(pdf.pages) 

df = []

for page in range(total_pages):
    table = pdf.pages[page].extract_table(table_settings=
    {"vertical_strategy": "text", "horizontal_strategy": "text"})
    for line in table:
        df.append(line)

df = pd.DataFrame(df)
df = df.fillna('')
df['merged'] = df[[0,1,2,3,4,5,6,7,8,9,10]].agg(''.join, axis=1)
df['no_nums'] = df['merged'].str.replace('\d+', '')
df[0] = pd.to_numeric(df[0], errors='coerce')
df[0] = df[0].fillna('xxxx')

output_ls = []
key_ls = []
section_flag = False
i = 0

for index, row in df.iterrows():
    section_test = isinstance(df[0].iloc[index], float)
    section_name = df['no_nums'].iloc[index]
    if section_test == True and section_name in ('.SUBMITTALS', '.CLOUSEOUT SUBMITTALS', '.WARRANTY'):
        section_flag = True
    else:
        section_flag = False

    if section_flag == True:
        for index, row in df.iloc[index:].iterrows():
            section_test = isinstance(df[0].iloc[index], float)
            if section_test == False:
                key_ls.append(section_name)
                output_ls.append(df['merged'].iloc[index])
                i = i+1
                section_test = isinstance(df[0].iloc[index], float)
                # print(df[0].iloc[index])
                # print(section_test)
                # print(i)
            if section_test == True and i > 0: 
                i = 0
                break
   

output_df = pd.DataFrame({'section': key_ls, 'notes': output_ls})
output_df.to_csv('test_doc.csv')
        
# df.to_csv('test_doc.csv')
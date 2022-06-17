import PyPDF2
import pandas as pd
import re

pdfFileObj = open('test_doc.pdf','rb')

pdfReader = PyPDF2.PdfFileReader(pdfFileObj)

pages = pdfReader.numPages

df = pd.DataFrame()
raw_line = []
line_no_nums = []
section_number = []

for i in range(pages):
    pageObj = pdfReader.getPage(i)
    page_number = i
    text = pageObj.extractText().split("\n")
    for i in range(len(text)):
            print(text[i],end="\n\n")
            line = text[i]
            raw_line.append(line)
            line_no_nums.append(re.sub('\d', '', line))
            section_number.append(re.sub('\D', '', line))

df['raw_line'] = raw_line
df['line_no_nums'] = line_no_nums
df['section_number'] = section_number

df.to_csv('test.csv')

pdfFileObj.close()
# Huw Waters
# 31/03/2022
# Code for extracting and calculating nosocomial COVID-19 from Public Health Wales Tableau Dashboards.

from tableauscraper import TableauScraper as TS
import pandas as pd

url_1 = r'https://public.tableau.com/views/RapidCOVID-19virology-Public/Hospitalonset'
url_2 = r'https://public.tableau.com/views/RapidCOVID-19virology-Public/Hospitalinpatients'

ts1 = TS()
ts2 = TS()
ts1.loads(url_1)
ts2.loads(url_2)
dashboard_1 = ts1.getWorkbook()
dashboard_2 = ts2.getWorkbook()

for t1 in dashboard_1.worksheets:
    print(t1.name)
    if t1.name == 'OHB Chart - Number by HB':
        df1 = t1.data

# df1.drop(['Health board-alias', 'Week ending date-alias', 'SUM(Number probable and definate HO)-alias', 'SUM(Proportion probable and definate HO)-alias'], inplace=True, axis=1)

df1 = df1[['Health board-value', 'Week ending date-value',
           'SUM(Proportion probable and definate HO)-alias']]

df1['Week ending date-value'] = pd.to_datetime(df1['Week ending date-value'])
df1.columns = ['healthboard', 'date', 'hai']
df1['hai'] = df1['hai'].astype('int')


for t2 in dashboard_2.worksheets:
    print(t2.name)
    if t2.name == 'P Chart - % positive inpatients by HB (2)':
        df2 = t2.data

df2.drop(['Health board-alias', 'Week ending date-alias', 'SUM(Count or percentage)-alias', 'SUM(Number positive inpatients)-alias',
         'SUM(Proportion positive inpatient)-alias', 'ATTR(Week ending date)-alias'], inplace=True, axis=1)
df2['Week ending date-value'] = pd.to_datetime(df2['Week ending date-value'])
df2.columns = ['healthboard', 'date', 'ai']
df2['ai'] = df2['ai'].astype('int')

df = df1.merge(df2, how='left', on=['healthboard', 'date'])


def calc_pc(df):
    if df['ai'] == 0:
        return 0
    else:
        calc = df['hai'] / df['ai']
        calc = calc*100
        calc = round(calc, 0)
        return calc


df['pc'] = df.apply(calc_pc, axis=1)

df.to_csv('hb.csv', index=False)

from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
from pypdf import PdfReader
import os
import glob


engine = create_engine("sqlite:///family.db")

months_dict = {
    'январь': 1,
    'февраль':2,
    'март':3,
    'апрель':4,
    'май':5,
    'июнь':6,
    'июль':7,
    'август':8,
    'сентябрь':9,
    'октябрь':10,
    'ноябрь':11,
    'декабрь':12
}

def reconstruct_period(month_name, year):
    '''parses the period of payment and returns it in datetime format'''

    month_val = [v for k,v in months_dict.items() if k == month_name]
    year_val = int(year)
    return datetime(year_val,month_val[0], 1)

def get_electric(file):
    '''parses pdf bill and returns cost of electricity for the period'''

    reader = PdfReader(file)
    page = reader.pages[0]
    metrics = page.extract_text().split('\n')
    
    for line in metrics:
        if 'СЧЁТ ЗА ЭЛЕКТРОЭНЕРГИЮ' in line:
            month = line.split('/')[1].split(' ')[1]
            year = line.split('/')[1].split(' ')[2].lower()
        elif 'Начислено за электроэнергию в расчётном периоде' in line:
            metric_value = float(line.split(':')[1].replace(',','.'))
        else:
            pass
    # return year,month,metric_value
    metric_period = reconstruct_period(month, year)
    # print(metric_period)
    metric_name = 'Электричество'
    
    return pd.DataFrame(
        {'period': [metric_period],
        'name': [metric_name],
        'value': [metric_value]
        })



def get_communals(file):
    '''parses utilities bill for the past month and returns cost of separate utility'''

    reader = PdfReader(file)
    page = reader.pages[0]
    text = page.extract_text().split('\n')[:54]
    text
    d = dict()

    for i in range(len(text)):
        if '(расчетный период)' in text[i]:
            month = text[i].split(' ')[0][2:].lower()
            year = int(text[i].split(' ')[1])
            metric_period = reconstruct_period(month, year)
        elif 'охранник' in text[i]:
            if text[i].endswith('по фактическим показаниям '):
                new_text = text[i][:-37]
                name = new_text.split(' шт ')[0]
                value = float(new_text.split(' ')[-1].replace(',','.'))
                d[name] = value
            else:
                name = text[i].split(' шт ')[0]
                value = float(text[i].split(' ')[-1].replace(',','.'))
                d[name] = value
        elif 'Гкал' in text[i]:
            if text[i].endswith('по фактическим показаниям '):
                new_text = text[i][:-37]
                name = new_text.split(' Гкал ')[0]
                value = round(float(new_text.split(' ')[4].replace(',','.')) + float(new_text.split(' ')[5].replace(',','.')),2)
                d[name] = value
            else:
                name = text[i].split(' Гкал ')[0]
                value = round(float(text[i].split(' ')[4].replace(',','.')) + float(text[i].split(' ')[5].replace(',','.')),2)
                d[name] = value
        elif 'кВт.ч' in text[i]:
            name = text[i-1].strip()
            if text[i].endswith('по фактическим показаниям '):
                new_text = text[i][:-37]
                value = float(new_text.split(' ')[-1].replace(',','.'))
                d[name] = value
            else:    
                value = float(text[i].split(' ')[-1].replace(',','.'))
                d[name] = value
        elif 'м3' in text[i]:
            if text[i].endswith('по фактическим показаниям '):
                new_text = text[i][:-37]
                name = new_text.split(' м3 ')[0]
                value = float(new_text.split(' ')[-1].replace(',','.'))
                d[name] = value
            else:
                name = text[i].split(' м3 ')[0]
                value = float(text[i].split(' ')[-1].replace(',','.'))
                d[name] = value
        elif 'м2' in text[i]:
            if text[i].endswith('по фактическим показаниям '):
                new_text = text[i][:-37]
                name = new_text.split(' м2 ')[0]
                value = float(new_text.split(' ')[-1].replace(',','.'))
                d[name] = value
            else:
                name = text[i].split(' м2 ')[0]
                value = float(text[i].split(' ')[-1].replace(',','.'))
                d[name] = value

    df = pd.DataFrame.from_dict(d, orient='index').reset_index(drop=False)
    df.columns = ['name', 'value']
    df['period'] = metric_period

    return df

if __name__ == '__main__':
    # getting list of files 
    dir = './source_pdf/'
    all_files = glob.glob(os.path.join(dir, '*.pdf'))
    main_df = pd.DataFrame()

    #getting main df
    for file in all_files:
        if 'electric' in file:
            tmp_df = get_electric(file)
            main_df = pd.concat([main_df, tmp_df])
        elif 'communal' in file:
            tmp_df = get_communals(file)
            main_df = pd.concat([main_df, tmp_df])
        else:
            pass
        # removing iterated file
        os.remove(file)

    main_df.reset_index(drop=True, inplace=True)

    # push to sql
    main_df.to_sql('utilities', con=engine, index=False, if_exists='append')



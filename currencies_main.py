import pandas as pd
from sqlalchemy import create_engine
import requests
from bs4 import BeautifulSoup
import datetime as dt
from credentials import BOT_TOKEN, CHAT_ID

engine = create_engine("sqlite:///family.db")
headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36'}
api = "https://api.telegram.org/bot"


def get_mir():

    # getting data from mir page
    url = 'https://mironline.ru/support/list/kursy_mir/'

    page = requests.get(url, headers=headers)
    soup = BeautifulSoup(page.content, 'html.parser')
    tds = soup.select('td')

    tds_list = []
    for el in tds:
        tds_list.append(el.text.strip())
    
    return float(tds_list[3].replace(',','.'))


def get_bnb(curr='usd'):
    # getting data from bnb page
    url = 'https://bnb.by/kursy-valyut/imbank/'

    page = requests.get(url, headers=headers)
    soup = BeautifulSoup(page.content, 'html.parser')
    trs = soup.select('tr')

    trs_list = []
    for el in trs:
        trs_list.append(el.text.strip())

    if curr == 'usd':
        return float(trs_list[3].replace('\t', '').replace('\n', ' ').split(' ')[-1])
    elif curr == 'eur':
        return float(trs_list[4].replace('\t', '').replace('\n', ' ').split(' ')[-1])

def get_paysett():
    url = 'https://paysett.ru/ru-us/otpravit-dengi/iz-rossii-v-belarus?fromCurrId=643&toCurrId=933&isFrom=true'

    page = requests.get(url, headers=headers)
    soup = BeautifulSoup(page.content, 'html.parser')
    spans = soup.select('span')

    curr = float(spans[19].text.split(' = ')[1].split(' ')[0])
    fee = 49.0
    curr_50k = (50000 + fee) / (50000 / curr)


    return curr, fee, curr_50k

def get_now():
    date = dt.datetime.now().strftime(format='%Y-%m-%d')
    time = dt.datetime.now().strftime(format='%H:%M:%S')
    return date, time

def send_message(message):

   url_req = api + BOT_TOKEN + "/sendMessage" + "?chat_id=" + CHAT_ID + "&text=" + message + "&parse_mode=HTML"
   results = requests.get(url_req)
   print(results.json())


if __name__ == '__main__':
    table_name = 'market_prices'
    mir = get_mir()
    bnb = get_bnb()
    mir_bnb = mir * bnb
    paysett_off, paysett_fee, paysett_50k = get_paysett()
    payset_bnb = paysett_50k * bnb
    date,time = get_now()

    data = {
        'dt': [date],
        'time': [time], 
        'mir':[mir],
        'paysett_official':[paysett_off], 
        'paysett_50k':[paysett_50k],  
        'bnb':[bnb], 
        'mir_bnb':[mir_bnb], 
        'paysett_bnb':[payset_bnb]
        }
    df = pd.DataFrame(data=data)
    df.to_sql(name=table_name,con=engine, if_exists='append',index=False)


    query = """select dt, time, bnb from market_prices"""
    df = pd.read_sql(query, engine)

    message = f"<pre> {df.tail(10).to_string(index=False)} </pre>"
    send_message(message)

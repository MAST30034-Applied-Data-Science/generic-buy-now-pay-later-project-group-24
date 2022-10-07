from urllib.request import urlretrieve  # for request data
import os

import pandas as pd
import zipfile

import ssl
ssl._create_default_https_context = ssl._create_unverified_context



def download_data(url,  filename, output_relative_dir='./data/tables/'):
    # download
    print('|> Downloading File...\n', url)
    urlretrieve(url, output_relative_dir+filename)
    return 




# SA2 boundary
print("\n|> Download External Dataset: SA2...")
# https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files#downloads-for-gda2020-digital-boundary-files
url = 'https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files/SA2_2021_AUST_SHP_GDA2020.zip'
download_data(url, 'external_SA2.zip')

zip_path = './data/tables/external_SA2.zip'
f = zipfile.ZipFile(zip_path, 'r') # 压缩文件位置
for file in f.namelist():
    # 解压到external_SA2
    f.extract(file, './data/tables/external_SA2/')
f.close()
print('|> Finished Download!')

if os.path.exists(zip_path):
    os.remove(zip_path)
    print('|> The zip file was deleted!')



# SA2 Data
# https://www.abs.gov.au/statistics/people/population/regional-population/2021#data-download
url = 'https://www.abs.gov.au/statistics/people/population/regional-population/2021/32180DS0001_2001-21.xlsx'
download_data(url, 'external_SA2_data.xlsx')
path = './data/tables/external_SA2_data'
data = pd.read_excel(path+'.xlsx', sheet_name='Table 1')
data.to_csv(path+'.csv')
os.remove(path+'.xlsx')



# POA boundary
print("\n|> Download External Dataset: POA...")
# https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files#downloads-for-gda2020-digital-boundary-files
url = 'https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files/POA_2021_AUST_GDA2020_SHP.zip'
download_data(url, 'external_POA.zip')

zip_path = './data/tables/external_POA.zip'
f = zipfile.ZipFile(zip_path, 'r') # 压缩文件位置
for file in f.namelist():
    # 解压到external_POA
    f.extract(file, './data/tables/external_POA/')
f.close()
print('|> Finished Download!')

if os.path.exists(zip_path):
    os.remove(zip_path)
    print('|> The zip file was deleted!')



# Postcode location
print("\n|> Download External Dataset: Postcode location...")
# https://github.com/Elkfox/Australian-Postcode-Data
url = 'https://raw.githubusercontent.com/Elkfox/Australian-Postcode-Data/master/au_postcodes.csv'
download_data(url, 'external_postcode.csv')



# retail_trade
print("\n|> Download External Dataset: retail_trade...")
# https://www.abs.gov.au/statistics/industry/retail-and-wholesale-trade/retail-trade-australia/jul-2022#survey-impacts-and-changes
url = 'https://www.abs.gov.au/statistics/industry/retail-and-wholesale-trade/retail-trade-australia/jul-2022/850101.xlsx'
download_data(url, 'external_retail_trade.xlsx')

retail_path = './data/tables/external_retail_trade'
data = pd.read_excel(retail_path+'.xlsx', sheet_name='Data1')
data.to_csv(retail_path+'.csv')
os.remove(retail_path+'.xlsx')



# Web crawling --- cases_daily_aus_NET
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from  selenium.webdriver.common.by import By
print("\n|> Download External Dataset: cases_daily_aus_NET...")
print('Please Enter Your Chomedriver Location: ')
chromedriver_loc = input()
# chromedriver_loc = '/Users/sukixuu/Downloads/chromedriver'


# 创建webDriver对象，指明使用chrome浏览器驱动
wd = webdriver.Chrome(service=Service(chromedriver_loc))

# 隐式等待,防止程序过快而网页反应不过来(5s)
wd.implicitly_wait(5)

# 下载state data
# 调用webDriver 对象的get方法，可以让浏览器打开指定网址
wd.get('https://infogram.com/1p20yj1nk2rqp7c0qq6de1r0egcrqv7e5kk?live')

# element = wd.find_element(By.ID, 'igc-tab-content1')
# element.click()

element = wd.find_element(By.CLASS_NAME, 'igc-data-download')
element.click()

wd.implicitly_wait(5)
wd.close()

print('Please Enter Your Chomedriver Download Location: ')
download_loc = input()
# download_loc = '/Users/sukixuu/Downloads'
download_loc = download_loc + '/cases_daily_aus_NET.csv'

renamed_loc = './data/tables/external_cases_daily_aus_NET.csv'
os.rename(download_loc, renamed_loc)





'''
Example:
➜  generic-buy-now-pay-later-project-group-24 git:(main) ✗ python3.7 ./scripts/download_external.py

|> Download External Dataset: SA2...
|> Downloading File...
 https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files/SA2_2021_AUST_SHP_GDA2020.zip
|> Finished Download!
|> The zip file was deleted!

|> Download External Dataset: SA2...
|> Downloading File...
 https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files/POA_2021_AUST_GDA2020_SHP.zip
|> Finished Download!
|> The zip file was deleted!

|> Download External Dataset: Postcode location...
|> Downloading File...
 https://raw.githubusercontent.com/Elkfox/Australian-Postcode-Data/master/au_postcodes.csv

|> Download External Dataset: retail_trade...
|> Downloading File...
 https://www.abs.gov.au/statistics/industry/retail-and-wholesale-trade/retail-trade-australia/jul-2022/850101.xlsx

|> Download External Dataset: cases_daily_aus_NET...
Please Enter Your Chomedriver Location: 
/Users/sukixuu/Downloads/chromedriver
Please Enter Your Chomedriver Download Location: 
/Users/sukixuu/Downloads/                        
'''
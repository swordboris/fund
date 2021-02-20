'''
Created on Feb 20, 2021

@author: borisw
'''


from database.dbCon import get_sqlite_conn
from bs4 import BeautifulSoup
from os.path import basename
import requests

from database.DBTable import DBTable
from database.FundInfoRecord import FundInfo


def get_fund_info(code):
    url = "http://fundf10.eastmoney.com/jjjz_{}.html".format(code)
    html = requests.get(url)
    html.encoding = 'utf-8'
    currentPage = BeautifulSoup(html.text, 'lxml')    

    
    dd = currentPage.find('dd', {'id': 'f10_menu_jjgs'})
    
    
    company_url = dd.find('a')['href']
    
    company_code = basename(company_url)[:-5]
    
    return company_code


if __name__ == "__main__":
    with get_sqlite_conn("fund") as conn:
        fundInfoTable = DBTable(conn,"FundInfo",FundInfo)
        df = fundInfoTable.getDataFrame(None, ['code'])
        for code in df.code.values:
            company_code = get_fund_info(code)
            fundInfoTable.updateRecords({'company_code':company_code}, "code='{}'".format(code))
            print(code,company_code)
            
            
        



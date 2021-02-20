'''
Created on Feb 20, 2021

@author: borisw
'''


import requests
import time
import js2py
from database.FundCompanyRecord import FundCompany
from database.DBTable import DBTable
from database.dbCon import get_sqlite_conn

from datetime import datetime

def get_fund_companies(table):
    time_ms = int(round(time.time() * 1000))
    url = "http://fund.eastmoney.com/Data/FundRankScale.aspx?_={}".format(time_ms)
    print(url)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36 QIHU 360SE',
        'Referer': 'http://fund.eastmoney.com/Company/default.html'
    }
    js_var = requests.get(url,headers=headers).text
    
    db = js2py.eval_js(js_var)
    
    for rec in db['datas']:

        fundCompanyRec = FundCompany()
        fundCompanyRec.code = rec[0]
        fundCompanyRec.name = rec[1] 
        fundCompanyRec.create_date = datetime.strptime(rec[2],'%Y-%m-%d').date()
        if rec[3].isnumeric():
            fundCompanyRec.fund_num = int(rec[3])
        fundCompanyRec.manager = rec[4]
        if len(rec[7]):
            fundCompanyRec.mgmt_money = float(rec[7])
        fundCompanyRec.star_level = len(rec[8])
        
        table.addRecords([fundCompanyRec])
        

    
    
    
if __name__ == "__main__":
    with get_sqlite_conn("fund") as conn:
        ## create table FundInfo 
        fundCompanyTable = DBTable(conn,"FundCompany",FundCompany)
        get_fund_companies(fundCompanyTable)
    
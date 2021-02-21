'''
Created on Feb 17, 2021

@author: borisw
'''

from datetime import datetime

from database.DBTable import DBTable
from database.dbCon import get_sqlite_conn
from database.FundInfoRecord import FundInfo

import requests
import js2py

  
def get_funds_info(code2company,table,pageIdx,pages=200):
    url = "http://fund.eastmoney.com/Data/Fund_JJJZ_Data.aspx?t=1&lx=1&letter=&gsid=&text=&sort=zde,asc&page={},{}&dt=1613400650082&atfc=&onlySale=0".format(pageIdx,pages)
    js_var = requests.get(url).text
    
    print(url)
    
    db = js2py.eval_js(js_var)
    
    total_pages = int(db['pages'])
    curpage = int(db['curpage'])
    datas = db['datas']
    
    #print("page download finished!", total_pages, curpage)

    for data in datas:
        fundInfo = FundInfo()
        fundInfo.code = data[0]
        fundInfo.name = data[1]
        
        try:
            fundInfo.net_per_unit = float(data[3])               
     
            fundInfo.net_acc = float(data[4])                
    
            fundInfo.daily_inc_val = float(data[7])
                
     
            fundInfo.daily_inc_pct = float(data[8])
                
            fundInfo.buy_ava = (data[9] == u'开放申购')
            fundInfo.sell_ava = (data[10] == u'开放赎回')
            
    
            fundInfo.fee = float(data[17][:-1])
            
            fundInfo.company_code = code2company[fundInfo.code]
        except:
            pass
        
        fundInfo.update_date = datetime.now().date()
        table.addRecords([fundInfo])
        
    if curpage < total_pages:
        get_funds_info(code2company,table,pageIdx + 1,pages)
        


if __name__ == '__main__':
    
    ## Create the tables
    with get_sqlite_conn("fund") as conn:
        ## create table FundInfo 
        fundInfoTable = DBTable(conn,"FundInfo",FundInfo)
        
            ## get company code from table
        df = fundInfoTable.getDataFrame(None,['code','company_code'])
        code2company = dict()
        for row in df.itertuples():
            code2company[getattr(row,'code')] = getattr(row,'company_code')
        
        print(code2company)
    
    
        get_funds_info(code2company,fundInfoTable, 1, 200)
        

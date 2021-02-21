'''
Created on Feb 21, 2021

@author: borisw
'''


import requests
import time
import json

from database.FundDailyRecord import FundDailyRec
from database.FundInfoRecord import FundInfo
from database.DBTable import DBTable
from datetime import datetime
from database.dbCon import get_sqlite_conn


def get_net_value(table,code, curr_page=1,total_num = 0):
    time_ms = int(round(time.time() * 1000))
    url = "http://api.fund.eastmoney.com/f10/lsjz?callback=&fundCode={}&pageIndex={}&pageSize=200&startDate=&endDate=&_={}".format(
    code,curr_page,time_ms)
    
    print(url)

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36 QIHU 360SE',
        'Referer': 'http://fundf10.eastmoney.com/jjjz_' + code + '.html'
    }

    js_var = requests.get(url, headers=headers).text
    
    jobj = json.loads(js_var)
    

    for item in jobj['Data']['LSJZList']:
        #print(total_num,item)
        
        rec = FundDailyRec()
        
        rec.date = datetime.strptime(item['FSRQ'],"%Y-%m-%d").date()
        if len(item['DWJZ']):
            rec.dwjz = float(item['DWJZ'])
        if len(item['LJJZ']):
            rec.ljjz = float(item['LJJZ'])
        if len(item['JZZZL']):
            rec.zzl = float(item['JZZZL'])
        table.addRecords([rec])        
        
        total_num += 1
       
    totalCount = jobj['TotalCount']
    pageIndex = jobj['PageIndex']
    pageSize = jobj['PageSize']
        
    if pageSize * pageIndex < totalCount:
        get_net_value(table,code, curr_page + 1, total_num)


if __name__ == "__main__":
    
    import os 
    
    ## create a file to record which fund has been updated
    today = datetime.now().date()
    
    filename = "daily_{}.log".format(today)
    
    fundCompleted = set()
    
    if os.path.exists(filename):
        f = open(filename,'r+')
        for code in f:
            fundCompleted.add(code.strip())
    else:
        f = open(filename,'w')
        
    
    with get_sqlite_conn("fund") as conn:
        ## create table FundInfo 
        
        fundInfoTab = DBTable(conn,"FundInfo",FundInfo) 
        
        df = fundInfoTab.getDataFrame(None, ['code'])  
        
        for code in df.code.values:
            
            if code not in fundCompleted:
                
                table = DBTable(conn,"FundDaily_{}".format(code),FundDailyRec)   
                get_net_value(table, code) 
                
                f.write("{}\n".format(code))  
                f.flush()
            
    f.close()
    













        
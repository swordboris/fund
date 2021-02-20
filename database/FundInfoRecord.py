'''
Created on Feb 20, 2021

@author: borisw
'''
from database.DBTable import DBRecord, DBTable
from datetime import date
from database.dbCon import get_sqlite_conn


## define the table fields for FundInfo
class FundInfo(DBRecord):
    code = ""
    name = ""
    net_per_unit = 0.0
    net_acc  = 0.0
    daily_inc_val = 0.0
    daily_inc_pct = 0.0 
    buy_ava = True 
    sell_ava = True 
    fee = 0.0 
    company_code = ""
    update_date = date(2021,2,16)    

    __keys__ = ['code']
    
    
if __name__ == '__main__':    
    ## Create the table
    with get_sqlite_conn("fund") as conn:
        ## create table FundInfo 
        fundInfoTable = DBTable(conn,"FundInfo",FundInfo)
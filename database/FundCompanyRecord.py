'''
Created on Feb 20, 2021

@author: borisw
'''

from database.DBTable import DBRecord, DBTable
from datetime import date
from database.dbCon import get_sqlite_conn


## define the table fields for FundInfo
class FundCompany(DBRecord):
    code = ""
    name = ""    
    create_date = date(2021,2,16) 
    fund_num = 0
    mgmt_money = 0.0
    manager = ""
    star_level = 0    

    __keys__ = ['code']
    
    
if __name__ == '__main__':    
    ## Create the table
    with get_sqlite_conn("fund") as conn:
        ## create table FundInfo 
        fundCompanyTable = DBTable(conn,"FundCompany",FundCompany)
        
        
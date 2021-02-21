'''
Created on Feb 21, 2021

@author: borisw
'''

from database.DBTable import DBRecord
from datetime import date


## define the table fields for FundInfo
class FundDailyRec(DBRecord):
    date = date(2021,2,16)    
    dwjz = 0.0
    ljjz = 0.0
    zzl = 0.0  

    __keys__ = ['date']
    

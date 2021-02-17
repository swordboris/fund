'''
@author: boris
'''

import sqlite3 
from myConfig import conf 

def get_sqlite_conn(dbname):  
    return sqlite3.connect(conf[dbname])
    


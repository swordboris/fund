'''
@author: boris
'''

from datetime import date, datetime
from datetime import time

SQLITE3_TYPE_MAPPING = {
        int:("int",lambda x:x),
        float:("real",lambda x:x),
        str:("text",lambda x:"'{}'".format(x)),
        date:("text",lambda x:"'{}'".format(x)),
        time:("text",lambda x:"'{}:{}:{}'".format(x.hour,x.minute,x.second)),
        datetime:("text",lambda x:"'{}'".format(x.strftime("%Y-%m-%d %H:%M:%S"))),
        bool:("int",lambda x:x)
}


class DBRecord(object):

    __keys__ = set()

    def __init__(self):
        super(DBRecord, self).__init__()

    @classmethod
    def getAttributes(cls):
        """
        :return: list. [(attribute name, python data type , its value and flag about is it key)]
        """
        attrs = []
        for k,v in cls.__dict__.items():
            if k.find("__") >= 0 or type(v).__name__ == "function":
                continue
            if k in cls.__keys__:
                isKey = True
            else:
                isKey = False

            attrs.append((k,type(v),v,isKey))
        return attrs

    def getAttrs(self):
        """
        :return: dict instance.
                 { attribute name : (python data type , its value)
                   ...
                 }
        """
        attrs = dict()
        for k,v in self.__class__.__dict__.items():
            if k.find("__") >= 0 or type(v).__name__ == "function":
                continue

            attrs[k] =(type(v),v)

        ## overwrite the class attribute
        for k,v in self.__dict__.items():
            if k.find("__") >= 0 or type(v).__name__ == "function":
                continue

            attrs[k] = (type(v), v)

        return attrs




    @classmethod
    def getKeys(cls):
        """
        :return: the set of keys name
        """

        return cls.__keys__
    
    


class DBTable(object):
    '''
    classdocs
    '''
    
    
    def __init__(self,dbConn,tableName,dbRecordClass):
            """
            :param dbConn:         the connection to sqlite3 database
            :param tableName:      the table name
            :param dbRecordClass:  the class derived from DBRecord
            """
            super(DBTable, self).__init__()
    
            self.connection = dbConn
            self.tableName = tableName
            self.dbRecordClass = dbRecordClass
            ## create the table in database if it does not exist
            sql = "create table if not exists {}".format(tableName)
            sql += "("
            ## for each attribute and its type
            attr_define_items = []
            keys = []
            for name,data_type,value,isKey in dbRecordClass.getAttributes():
                pgtype,func = SQLITE3_TYPE_MAPPING[data_type]
                attr_define_items.append("{} {} default {}".format(name,pgtype,func(value)))
                if isKey:
                    keys.append(name)
    
            sql += ",".join(attr_define_items)
            if len(keys):
                sql += ",CONSTRAINT {}_key PRIMARY KEY (".format(tableName)
                sql += ",".join(keys)
                sql += ")"
    
            sql += ")"
    
    
            self.__execute_sql__(sql)
            
            
    def addRecords(self,dbRecords):
        """
        :param dbRecords:  list of instance for class dbRecord
        :return:
        """

        if len(dbRecords) == 0:
            return

        for record in dbRecords:
            if not isinstance(record,DBRecord):
                raise Exception("The parameter is not instance of class {}".format(self.__class__.__name__))

        ## save the dbRecords to table
        ## insert if do not collison with the keys
        ## insert into <table> (field1, field2, ...) values (value1,value2,...) on conflict(key1,key2,...)
        ## do update set <field1> = excluded.<field1>, ...


        fields = list()
        values_strs = []
        for rec in dbRecords:
            attrs = rec.getAttrs()
            values = []
            for k,(tv,v) in attrs.items():
                pgtype, func = SQLITE3_TYPE_MAPPING[tv]
                if k not in fields:
                    fields.append(k)
                values.append(str(func(v)))
            values_str = ",".join(values)

            values_strs.append("({})".format(values_str))

        sql_values = ",".join(values_strs)


        keys = dbRecords[0].__class__.getKeys()

        update_fields=[]
        for field in fields:
            update_fields.append("{}=excluded.{}".format(field,field))

        sql_update_fields = ",".join(update_fields)
        #print sql_update_fields

        if len(keys):

            sql = """ insert into {} ({}) values {} on conflict ({}) do update set {}
            
            """.format(self.tableName,
                       ",".join(fields),sql_values,",".join(keys),sql_update_fields)
        else:
            sql = "insert into {} ({}) values {}".format(self.tableName,",".join(fields),sql_values)

        self.__execute_sql__(sql)
        
    def deleteRecords(self,condition):
        sql = "delete from {} where {}".format(self.tableName,condition)
        self.__execute_sql__(sql)
        

    
    def updateRecords(self,fields2Value,condition=None):
        ## update table set () where condition
        
        ops = []
        for name,data_type,value,isKey in self.dbRecordClass.getAttributes():
            if name in fields2Value.keys():
                pgtype, func = SQLITE3_TYPE_MAPPING[data_type]
                ops.append("{} = {}".format(name,func(fields2Value[name])))
                
        if condition is not None:
            sql = "update {} set {} where {}".format(self.tableName,",".join(ops),condition)
        else:
            sql = "update {} set {}".format(self.tableName,",".join(ops))
        
        self.__execute_sql__(sql)
                
 
    
    def drop(self):
        sql = "drop table {}".format(self.tableName)
        self.__execute_sql__(sql)
        
    def getDataFrame(self,condition,fields='*'):
        import pandas as pd 
        if condition is None:
            condition = ""
        else:
            condition = "where {}".format(condition)
            
        if isinstance(fields, list):
            sql = "select {} from {} {}".format(",".join(fields),self.tableName,condition)
        else:
            sql = "select * from {} {}".format(self.tableName,condition)
        #print(sql)
        return pd.read_sql(sql,self.connection)
        


    def __execute_sql__(self,sql):
        #print(sql)
        dbConn = self.connection

        cursor = dbConn.cursor()

        try:
            cursor.execute(sql)
        except sqlite3.OperationalError:
            pass
            




import sys
import pyodbc
import configparser
import pandas as pd

class ChicWDB():
    parser = configparser.ConfigParser()
    parser.read('ChicWeight_serv.ini')
    SQL_driver = parser['myserv']['SQL_driver']
    Server = parser['myserv']['Server']
    Database = parser['myserv']['Database']
    # SQL_driver =SQL Server Native Client 11.0
    # Server = DESKTOP-04LQB9S\SQLEXPRESS
    # Database = forMpred
    # uid = sa
    # pwd = p@ssw0rd
    # con_string = f'Driver={SQL_driver};Server={Server};Database={Database};UID={uid};pwd={pwd}'

    def __init__(self):
        con_string = f'Driver={self.SQL_driver};Server={self.Server};Database={self.Database};Trusted_Connection=yes;'
        try:
            self.conn = pyodbc.connect(con_string)
        except Exception as e:
            print(e)
            print('Task is terminate')
            sys.exit()
        else:
            self.cursor = self.conn.cursor()
            print('db conection successfully')
            
    def create_chic_weigt_tbl(self):
        create_chic_weigt_sql = """CREATE TABLE ChicW (
            ID int NOT NULL IDENTITY(1,1),
            files varchar(20),
            number varchar(20),
            date_time datetime,
            weight varchar(10),
            Sex_Limit_Category varchar(20),
            species varchar(2),
            farm varchar(5),
            house varchar(5),
            sex varchar(5),
            pen varchar(5)
            )
            """
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(create_chic_weigt_sql)
        except Exception as e:
            self.cursor.rollback()
            print(e)
            print('create_chic_weigt_tbl error')
        else:
            print('Create chic_weigt table successful')
            self.cursor.commit()
            self.cursor.close()
            
    def insert_chic_weigt_tbl(self,data):
        insert_chic_weigt_sql = """INSERT INTO ChicW (
            files,
            number,
            date_time,
            weight,
            Sex_Limit_Category,
            species,
            farm,
            house,
            sex,
            pen
            ) 
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(insert_chic_weigt_sql, data)
        except Exception as e:
            self.cursor.rollback()
            print(e)
            print('insert_chic_weigt_tbl error')
        else:    
            self.cursor.commit()
            self.cursor.close() 
            
    def get_chic_weigt_tbl(self):
        get_chic_weigt_sql = """SELECT * from ChicW
        """
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(get_chic_weigt_sql)
            data = self.cursor.fetchall()
            data = pd.DataFrame((tuple(t) for t in data))
            data = data.rename(columns={
                0:'ID',
                1:'files',
                2:'number',
                3:'date_time',
                4:'weight',
                5:'Sex_Limit_Category',
                6:'species',
                7:'farm',
                8:'house',
                9:'sex',
                10:'pen'
                })
        except Exception as e:
            self.cursor.rollback()
            print(e)
            print('get_chic_weigt_tbl error')
        else:
            self.cursor.commit()
            self.cursor.close()
            return  data       
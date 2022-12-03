import pandas as pd
from db import ChicWDB

db = ChicWDB()

class Chicken_Weigth():
    def __init__(self) -> None:
        pass
    
    def weight_pre(self):
        data = pd.read_csv('JC06 ALL.csv' , encoding = 'utf-16',delimiter='\t',skip_blank_lines='True', header=None)
        data = data[data[6].apply(lambda x: (x != 'CV [%]:') and (x != 'Uniformity [%]:') and (x != 'Speed [1/hour]:'))]
        data = data[data[0].apply(lambda x: (x != 'File'))]
        data = data.dropna(axis=0, how='all')
        data = data.dropna(axis=1, how='all')
        data = data.rename(columns={
            0:'file',
            1:'number',
            2:'date_time',
            3:'weight',
            4:'Sex_Limit_Category'   
        })
        data['species'] = data['file'].str[0]
        data['farm'] = data['file'].str[1:2]
        data['house'] = data['file'].str[2:4]
        data['gender'] = data['file'].str[4:5]
        data['pen'] = data['file'].str[5:7]
        return data
    
    def update_weigth_to_db(self):
        try:
            Chicw = self.weight_pre()
            for i in range(len(Chicw)):
                Chicws = Chicw.values.tolist()
                Chicws = Chicws[i][0],Chicws[i][1],Chicws[i][2],Chicws[i][3],Chicws[i][4],Chicws[i][5],Chicws[i][6],Chicws[i][7],Chicws[i][8],Chicws[i][9]
                db.insert_chic_weigt_tbl(Chicws)
            print('update_weigth_to_db successfully')
        except:
            print('update_weigth_to_db error')
        
from db import ChicWDB
from data_processing import Chicken_Weigth

db = ChicWDB()
dps = Chicken_Weigth()


if __name__=="__main__":
    try:
        dps.update_weigth_to_db()
    except:
        print('update_weigth_to_db error')
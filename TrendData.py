import sqlalchemy
from sqlalchemy import create_engine
import pandas as pd
import pytrends
import os

#Initial Configuration of connection
def init_connection_engine():
    db_config = {
        "pool_size": 5,
        "max_overflow": 2,
        "pool_timeout": 30,
        "pool_recycle": 1800,
    }
    return init_unix_connection_engine(db_config)
        
#Unix connection engine
def init_unix_connection_engine(db_config):
    
    #Variable and instances for DB MySql instance - sql-firstproject
    connection_name = os.environ["tough-victor-313209:europe-west3:sql-firstproject"]
    db_name = os.environ["demo"]
    db_user = os.environ["san"]
    db_password = os.environ[""]
    
    # UNIX socket for MySql connection
    driver_name = "mysql+pymysql"
    query_string = dict({"unix_socket": "/cloudsql/{}".format(connection_name)})

    pool= sqlalchemy.create_engine(
      sqlalchemy.engine.url.URL(
        drivername=driver_name,
        username=db_user,
        password=db_password,
        database=db_name,
        query=query_string,
      ),
      **db_config
    )
    return pool

#Pytrend configuration and establishing connection to google
from pytrends.request import TrendReq
pytrends = TrendReq(hl='en-US', tz=360)

#Function entry point
def insert(request):
    request_json = request.get_json()
    db = init_connection_engine()
    
    with db.connect() as dbConnection:
        try:
            #Building a pytrend Payload
            keywords = ['Silver', 'Gold', 'denim', 'Wool', 'fir', 'cedar', 'Python', 'Java']
            key = []
            for i in keywords:
                key.append([i])
            
            for i in range(0, len(keywords)):
                pytrends.build_payload(kw_list=key[i], cat=0, timeframe='now 1-d', geo='US', gprop='')
                data = pytrends.interest_over_time().drop(columns='isPartial')
                if i == 0:
                    data1 = data
                else:
                    data1= pd.merge(data1, data, on='date')
            
            #Framing the data and givin to SQL
            finalData = pd.DataFrame(data1)
            tableName = "TrendData"
            dbConnection.execute("DROP TABLE IF EXISTS TrendData;")
            frame = finalData.to_sql(tableName, dbConnection, if_exists='fail');
                      
        except ValueError as vx:
            print(vx)
        except Exception as ex:
            print(ex)
        else:
            print("Table %s created successfully."%tableName);
        finally:
            dbConnection.close()

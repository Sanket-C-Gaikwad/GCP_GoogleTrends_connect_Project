import sqlalchemy
from sqlalchemy import create_engine
import pytrends


#Initial Configuration engine DB
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
    
    #Variable and instances for DB MySql
    connection_name = "tough-victor-313209:europe-west3:sql-firstproject"
    db_name = "demo"
    db_user = "san"
    db_password = ""
    
    # UNIX socket for MySql connection
    driver_name = 'mysql+pymysql'
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

#Pytrend configuration
#Connecting to Google
from pytrends.request import TrendReq
pytrends = TrendReq(hl='en-US', tz=360)
#Building a Payload
all_keywords = ['Silver', 'Gold', 'Wool', 'Silk', 'fur', 'cedar', 'python', 'java']
keywords = []
all_data = []

#Function to check trend
def check_trends(key):
    item = key
    pytrends.build_payload(keywords, cat=0, timeframe='now 1-d', geo='', gprop='')
    data = pytrends.interest_over_time() #Data every 7 minutes of interval
    data = data.drop(labels=['isPartial'],axis='columns') #Removing extra columns
    meand = int(data.mean())
    wordd = item

    stmt = sqlalchemy.text(
        "INSERT INTO trendDataMean (count, keyword)"
        " VALUES (:mean_v, :word_v)"
    )
    
    db = init_connection_engine()
    try:
        with db.connect() as conn:
            conn.execute(stmt, mean_v=meand, word_v=wordd)
    except Exception as e:
        return 'Error: {}'.format(str(e))
    return 'ok'


#Entry Point Function
def insert(request):
    request_json = request.get_json()
    
    # Creating and Cleaning the table for new update
    db = init_connection_engine()
    with db.connect() as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS trendDataMean "
            "( count INT NOT NULL,keyword VARCHAR(50) NOT NULL);"
        )
        conn.execute("TRUNCATE TABLE trendDataMean;")
    
    for key in all_keywords:
        keywords.append(key)
        check_trends(key)
        keywords.pop()

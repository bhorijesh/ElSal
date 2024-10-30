from decouple import config
from mysql import connector 

db_user = str(config("SERVER_DB_USER", cast=str))
db_password = str(config("SERVER_DB_PASSWORD", cast=str))
db_host = str(config("SERVER_DB_HOST", cast=str))
db_port = config("SERVER_DB_PORT", cast=int)
db_name = str(config("SERVER_DB_NAME", cast=str))


def connect_database(
     autocommit = False,
     user : str = db_user,
     password : str = db_password,
     host : str = db_host,
     port : int = db_port,
     database : str = db_name
):
     try:
          print('Database : ', db_name)
          db_connect = connector.connect(
               user = user,
               password = password,
               host = host,
               port = port,
               database = database,
               auth_plugin = 'mysql_native_password',
          )
          db_connect.autocommit = autocommit
          cursor = db_connect.cursor(dictionary=True)
          return db_connect, cursor
          
     except Exception as error:
          print(f'Error while connecting database : {error}')
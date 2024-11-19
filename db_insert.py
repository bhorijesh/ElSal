import pandas as pd
import os 
import sys

# Starting from the current script
current_dir = os.path.dirname(__file__)
elsal = os.path.abspath(os.path.join(current_dir, '..'))

# Add elsal directory to sys.path
sys.path.append(elsal)

from base.db_connect import connect_database


def insert_into_table(cursor, table):
    df = pd.read_csv('details.csv')
    df.fillna(
          {
          'price' : 0,
          'area_square_vara' : 0,
          'construction_area' : 0,
          'Bedroom' : 0,
          'full_bathrooms' : 0,
          'half_bathrooms' : 0,
          'parking' : 0,
          'levels' : 0,
          'previous_price': 0,
     
          },
          
          inplace=True
     )
        
    df = df.fillna('NULL')

     
    placeholders = ', '.join(['%s'] * len(df.columns))
    columns = ', '.join(df.columns)
    insert_query = f'INSERT INTO {table} ({columns}) VALUES ({placeholders})'
     
    try:
          # iteration through each row
          for index, row in df.iterrows():
               cursor.execute(insert_query, tuple(row))
    except Exception as e:
          print(f'Error while inserting into Database: {e}')


def main():
     db_connections, cursor = connect_database()
     
     table = 'ElSal'
     
     insert_into_table(cursor, table)
     db_connections.commit()  # Commit changes to the database
     
     print('Data Insertion into Database Completed')
     

if __name__ == '__main__':
     main()

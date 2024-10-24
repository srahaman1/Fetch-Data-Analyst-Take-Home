import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import os

engine = create_engine(f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}")

csv_directory = 'Fetch-Data-Analyst-Take-Home/Cleaned Files'

with Session(engine) as session:
    for filename in os.listdir(csv_directory):
        if filename.endswith('.csv'):
            filepath = os.path.join(csv_directory, filename)
            # Read CSV file into a pandas DataFrame
            df = pd.read_csv(filepath)
            # Table name (you can customize this)
            table_name = os.path.splitext(filename)[0].lower()
            print(f'Loading {table_name}...')
            with session.begin():
                try:
                    df.to_sql(table_name, engine, if_exists='replace', chunksize=10000, index=False)
                except Exception as e:
                    print(f"Error occurred while processing {filename}: {e}")
                    print(f"Exception details: {e.__class__.__name__} - {e}")
                    session.rollback()
                else:
                    session.commit()
                    print(f"{filename} loaded successfully!")
    session.close()  # Ensure session is closed
    print('Session Closed')
    engine.dispose()
    print('Engine Closed')
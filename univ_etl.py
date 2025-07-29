import os
from dotenv import load_dotenv
import requests
import pandas as pd
from sqlalchemy import create_engine


"""
Python ETL Universities
"""

def extract():
    """
      This API extracts data fromhttp://universities.hipolabs.com
    """
    api_url = "http://universities.hipolabs.com/search?country=Philippines"
    data = requests.get(api_url).json()
    
    return data

def transform(data):
    """
    This function transforms that data extracted from api_url
    """
    df = pd.DataFrame(data)
    print(f"total number of universities from API {len(data)}")
    df = df[df["state-province"].fillna("").str.contains("National Capital Region")]
    print(f" the number of universities in NCR {len(df)}")
    df["domains"] = [','.join(map(str, l)) for l in df["domains"]]
    df["web_pages"] = [','.join(map(str, l)) for l in df["web_pages"]]
    df = df.reset_index(drop=True)
    return df[["domains", "country", "web_pages", "name"]]

def load(df):
    """
    loads data into a sqlite database
    """
    load_dotenv()

    username = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    database = os.getenv("DB_NAME")

    print(f"Connected as: {username}@{host}/{database}")

    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"

    engine = create_engine(postgres_url)
    df.to_sql('ncr_uni', engine, if_exists='replace', index=False)


data = extract()
df = transform(data)
load(df)
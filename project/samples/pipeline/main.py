import requests
import pandas as pd
from sqlalchemy import create_engine


class MyPipeline:
    def extract(self) -> dict:
        API_URL = (f"http://universities.hipolabs.com/search?country=United+States")
        data =  requests.get(API_URL).json()
        return data

    def transform(self, data: dict) -> pd.DataFrame:
        """ Transforms the dataset into desired structure and filters"""
        df = pd.DataFrame(data)
        return df[["domains","country","web_pages","name"]]

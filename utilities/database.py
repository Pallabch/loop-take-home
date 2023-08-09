# import pymongo
from urllib.parse import quote_plus
import pandas as pd
from dotenv import load_dotenv
import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

load_dotenv()


username = 'data_store'
password = 'qwerty123'
cluster = 'cluster0'

class Database:
    def __init__(self):
        self.username = os.getenv('username')
        self.password = os.getenv('password')
        self.cluster = os.getenv('cluster')
        # Create a new client and connect to the server
        self.uri = "mongodb+srv://" + username  + ':' + password + "@" + cluster + ".pgcu0ym.mongodb.net/?retryWrites=true&w=majority"
        # Create a new client and connect to the server
        self.client = MongoClient(self.uri, server_api=ServerApi('1'))
        self.cache = os.getenv('CACHE')
    def __str__(self):
        return "Fetched data from mongodb"
    def fetch_menu_hours(self,location):
        self.client = MongoClient(self.uri, server_api=ServerApi('1'))
        db = self.client.Loop_Data
        mycol = db["menu_hours"]
        x = list(mycol.find())
        df = pd.DataFrame(x)
        if not os.path.exists(self.cache):
            os.mkdir(self.cache)
        df.to_csv(os.getenv('FILE_MENU_HOURS'))
        return
    def fetch_store_status(self,location):
        self.client = MongoClient(self.uri, server_api=ServerApi('1'))
        db = self.client.Loop_Data
        mycol = db["store_status"]
        x = list(mycol.find())
        df = pd.DataFrame(x)
        if not os.path.exists(self.cache):
            os.mkdir(self.cache)
        df.to_csv(os.getenv('FILE_STORE_STATUS'))
        return
    def fetch_time_zones(self,location):
        self.client = MongoClient(self.uri, server_api=ServerApi('1'))
        db = self.client.Loop_Data
        mycol = db["time_zones"]
        x = list(mycol.find())
        df = pd.DataFrame(x)
        if not os.path.exists(self.cache):
            os.mkdir(self.cache)
        df.to_csv(os.getenv('FILE_TIME_ZONES'))
        return
    def close_connection(self):
        self.client.close()
        return

if __name__ == '__main__':
    data = Database()
    data.fetch_menu_hours(os.getenv('FILE_MENU_HOURS'))
    data.fetch_store_status(os.getenv('FILE_STORE_STATUS'))
    data.fetch_time_zones(os.getenv('FILE_TIME_ZONES'))


        




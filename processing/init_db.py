from mt_mesonet_satellite import MesonetSatelliteDB
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

conn = MesonetSatelliteDB(
    uri=os.getenv("Neo4jURI"),
    user=os.getenv("Neo4jUser"),
    password=os.getenv("Neo4jPassword")
)

conn.init_db_indices()
dat = pd.read_csv("/home/cbrust/git/mt-mesonet-satellite/data/clean_format.csv")
conn.init_db("/home/cbrust/git/mt-mesonet-satellite/data/db_init")
conn.post(dat)
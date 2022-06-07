import pandas as pd
from pathlib import Path
from Product import Product
import janitor

def clean_modis(pth):
    product = ".".join(pth.stem.split("-")[-3:-1])
    product = Product(product)
    valid_cols = ['NDVI', 'EVI', 'ET', 'PET', 'Fpar', 'Lai', 'Gpp']
    select_cols = ['ID', 'Latitude', 'Longitude', 'Date']

    dat = pd.read_csv(pth)
    
    valid_cols = [x for y in valid_cols for x in dat.columns if y in x]
    valid_cols = [x for x in valid_cols if "QC" not in x]

    final_cols = select_cols + valid_cols
    dat = dat[final_cols]
    filt = dat[valid_cols] > -1000
    dat =  dat[filt.iloc[:, 0]]

    dat = dat.pivot_longer(
        index = "Date", 
        column_names = valid_cols, 
        names_to = "variable",
        values_to = "value"
    )

    #TODO: Implement multiplication by scale factors. 

    return dat
    

def clean_all_modis(dirname):

    myd = [x for x in Path(dirname).glob("*MYD*")]
    mod = [x for x in Path(dirname).glob("*MOD*")]

    dfs = [clean_modis(x) for x in (myd+mod)]
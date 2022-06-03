import pandas as pd
from pathlib import Path
import janitor

def clean_modis(pth):
    print(pth)
    valid_cols = ['NDVI', 'EVI', 'ET', 'PET', 'Fpar', 'Lai', 'Gpp']
    select_cols = ['ID', 'Latitude', 'Longitude', 'Date']

    dat = pd.read_csv(pth)
    
    valid_cols = [x for y in valid_cols for x in dat.columns if y in x]
    valid_cols = [x for x in valid_cols if "QC" not in x]

    final_cols = select_cols + valid_cols
    dat = dat[final_cols]
    filt = dat[valid_cols] > -1000
    dat =  dat[filt.iloc[:, 0]]

    # TODO: Get pyjanitor working, then use 
    # dat.pivot_longer(index = "date", column_names = valid_cols, names_to = "variable", values_to = "value")

    tmp_dfs = []
    for x in valid_cols:

        dat[select_cols + [x]]


def clean_all_modis(dirname):

    myd = [x for x in Path(dirname).glob("*MYD*")]
    mod = [x for x in Path(dirname).glob("*MOD*")]

    dfs = [clean_modis(x) for x in (myd+mod)]
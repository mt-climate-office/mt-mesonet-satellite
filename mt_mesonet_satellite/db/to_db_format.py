import argparse
import pandas as pd
from pathlib import Path
from typing import Union, Optional
import numpy as np

def to_db_format(
    f: Union[str, Path], neo4j_pth: Union[str, Path] = "/var/lib/neo4j/import/", out_name: Optional[str]=None
) -> None:
    """Convert dates to unix timestamps, clean element names, and save data to neo4j import directory.
       for Ubuntu machines, the defaults is /var/lib/neo4j/import/

    Args:
        f (Union[str, Path]): Path to raw master_db.csv file.
        neo4j_pth (Union[str, Path], optional): Neo4j import directory location. Defaults to "/var/lib/neo4j/import/".
    """

    dat = pd.read_csv(f)
    dat = dat.assign(Date=pd.to_datetime(dat["Date"], utc=True))
    dat = dat.assign(
        Date=(dat["Date"] - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta("1s")
    )
    dat = dat.rename(
        columns={"ID": "station", "Date": "timestamp", "product": "platform"}
    )

    dat = dat.replace(
        {
            "element": {
                "ET_500m": "ET",
                "Fpar_500m": "Fpar",
                "GPP_gpp_mean": "GPP",
                "Geophysical_Data_sm_rootzone": "sm_rootzone",
                "Geophysical_Data_sm_rootzone_wetness": "sm_rootzone_wetness",
                "Geophysical_Data_sm_surface": "sm_surface",
                "Geophysical_Data_sm_surface_wetness": "sm_surface_wetness",
                "Gpp_500m": "GPP",
                "Lai_500m": "LAI",
                "PET_500m": "PET",
                "_500m_16_days_EVI": "EVI",
                "_500m_16_days_NDVI": "NDVI",
            }
        }
    )
    dat = dat.assign(id = dat.station + '_' + dat.timestamp.astype(str) + '_' + dat.platform + '_' + dat.element)
    print("Data succesfully reformatted,")
    dat = dat.drop_duplicates()
    out_name = Path(f).stem if not out_name else out_name
    groups = dat.groupby(np.arange(len(dat.index))//5000)
    for (num, tmp) in groups:
        tmp_name = f"{out_name}_{num}.csv"
        tmp.to_csv(Path(neo4j_pth) / tmp_name)

if __name__ == '__main__':

    parser = argparse.ArgumentParser("Convenience to help run as root.")
    parser.add_argument(
        '-f', '--file', type=Path, help='master_db.csv file to reformat'
    )
    parser.add_argument(
        '-od', '--outdir', type=Path, help='Neo4j directory to save dataframe to.'
    )
    parser.add_argument(
        '-on', '--outname', type=Path, help='Filename to use for the output file.', default="data_init"
    )
    args = parser.parse_args()

    to_db_format(args.file, args.outdir, args.outname)
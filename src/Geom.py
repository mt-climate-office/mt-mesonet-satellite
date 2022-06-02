from dataclasses import dataclass
import pandas as pd
from typing import List
import geopandas as gpd


@dataclass
class Point:
    lats: List[int]
    lons: List[int]
    ids: List[str]

    def task_format(self):
        return [
            {"latitude": x, "longitude": y, "id": z}
            for x, y, z in zip(self.lats, self.lons, self.ids)
        ]

    @classmethod
    def from_mesonet(cls):
        dat = pd.read_csv("https://mesonet.climate.umt.edu/api/v2/stations/?type=csv")
        return cls(
            lats=dat.latitude.tolist(),
            lons=dat.longitude.tolist(),
            ids=dat.station.tolist(),
        )

    @classmethod
    def from_geojson(cls, pth, id_col):
        dat = gpd.read_file(pth)
        return cls(
            lats=dat.geometry.y.tolist(),
            lons=dat.geometry.tolist(),
            ids=dat[id_col].tolist(),
        )


@dataclass
class Poly:
    raise NotImplementedError("This class is not implemented yet")

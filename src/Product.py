from dataclasses import dataclass, field
from typing import List, Optional, Dict
import requests


@dataclass
class Layer:
    AddOffset: Optional[float]
    Available: bool
    DataType: str
    Description: str
    Dimensions: List[str]
    FillValue: int
    IsQA: bool
    Layer: str
    OrigDataType: str
    OrigValidMax: int
    OrigValidMin: int
    QualityLayers: str
    QualityProductAndVersion: str
    ScaleFactor: Optional[int]
    Units: str
    ValidMax: int
    ValidMin: int
    XSize: int
    YSize: int


@dataclass
class Product:
    product: str
    layers: Dict[str, Layer] = field(init=False)

    def __post_init__(self):
        self.layers = self.get_layers()

    def get_layers(self):
        response = requests.get(
            "https://appeears.earthdatacloud.nasa.gov/api/product/{0}".format(
                self.product
            )
        )
        layer_response = response.json()

        return {k: Layer(**v) for k, v in layer_response.items()}

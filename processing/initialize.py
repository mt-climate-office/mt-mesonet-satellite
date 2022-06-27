from mt_mesonet_satellite import Point
from mt_mesonet_satellite import Session
from mt_mesonet_satellite import Task, Submit


import datetime as dt

stations = Point.from_mesonet()
session = Session()
aqua = Submit(
    name="mesonet_aqua_download",
    products=[
        "MYD13A1.061",
        "MYD13A1.061",
        "MYD15A2H.061",
        "MYD15A2H.061",
        "MYD16A2.061",
        "MYD16A2.061",
        "MYD17A2H.061",
    ],
    layers=[
        "_500m_16_days_NDVI",
        "_500m_16_days_EVI",
        "Fpar_500m",
        "Lai_500m",
        "ET_500m",
        "PET_500m",
        "Gpp_500m",
    ],
    start_date="2022-06-01",
    end_date=str(dt.date.today()),
    geom=stations,
)

terra = Submit(
    name="mesonet_terra_download",
    products=[
        "MOD13A1.061",
        "MOD13A1.061",
        "MOD15A2H.061",
        "MOD15A2H.061",
        "MOD16A2.061",
        "MOD16A2.061",
        "MOD17A2H.061",
    ],
    layers=[
        "_500m_16_days_NDVI",
        "_500m_16_days_EVI",
        "Fpar_500m",
        "Lai_500m",
        "ET_500m",
        "PET_500m",
        "Gpp_500m",
    ],
    start_date="2000-01-01",
    end_date=str(dt.date.today()),
    geom=stations,
)


smap = Submit(
    name="mesonet_smap_download",
    products=[
        "SPL4CMDL.006",
        "SPL4SMGP.006",
        "SPL4SMGP.006",
        "SPL4SMGP.006",
        "SPL4SMGP.006",
    ],
    layers=[
        "GPP_gpp_mean",
        "Geophysical_Data_sm_surface",
        "Geophysical_Data_sm_surface_wetness",
        "Geophysical_Data_sm_rootzone",
        "Geophysical_Data_sm_rootzone_wetness",
    ],
    start_date="2000-01-01",
    end_date=str(dt.date.today()),
    geom=stations,
)

aqua.launch(token=session.token)
terra.launch(token=session.token)
smap.launch(token=session.token)

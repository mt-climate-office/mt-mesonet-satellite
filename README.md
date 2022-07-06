# MT-MESONET-SATELLITE
A Python package to extract NASA satellite data at Montana Climate Office Mesonet stations using the [AppEEARS API](https://appeears.earthdatacloud.nasa.gov/api/?python) and store it in a [Neo4j](https://neo4j.com/) database. The AppEEARS API requires a NASA Earthdata login ([https://urs.earthdata.nasa.gov/users/new](https://urs.earthdata.nasa.gov/users/new)). 

To stand up the database and run daily automatic updates, use `docker compose`:

`docker compose up --build -d`.

This requires a `.env` file with the following credentials defined:
 - NEO4J_AUTH: The credentials to give the Neo4j database, formatted as {username}/{password}
 - EarthdataLogin: Your NASA Earthdata username (do not include email).
 - EarthdataPassword: Your NASA Earthdata password. 
 - Neo4jUser: The Neo4j user defined in NEO4J_AUTH
 - Neo4jURI: bolt://{container_name}, where container_name is the name of the Docker container hosting the Neo4j database.
 - Neo4jPassword: The password defined in NEO4J_AUTH. 

To use the functionality of the package, you can install from GitHub using `pip` or `poetry`:

`pip install git+https://github.com/colinbrust/mt-mesonet-satellite.git@main`

or using poetry:

`poetry add git+https://github.com/colinbrust/mt-mesonet-satellite.git#main`

Below is an example of how to use the package: 

```python
from mt_mesonet_satellite import Point, Session, Submit, list_task
import datetime as dt

# login to an AppEEARS session
session = Session(dot_env=True)

# Make a Point object with the locations of MT Mesonet stations.
stations = Point.from_mesonet()

# Make a Submit object for MOD16 ET and PET data. This Submit object will allow you to aggregate and download the data.
task = Submit(
    name="mesonet_aqua_download",
    products=[
        "MYD16A2.061",
        "MYD16A2.061",
    ],
    layers=[
        "ET_500m",
        "PET_500m",
    ],
    start_date="2020-01-01",
    end_date=str(dt.date.today()),
    geom=stations,
)

# Launch the submission
task.launch(token=session.token)

# Depending on the start and end date of your task, as well as the number of points you are extracting, it can take up to a day for all the data to download.

# You can check the status of the task by using the status_update method. If the task is complete, it will return 'done', or 'pending' if the task is still running:
print(task.status_update())

# If you are still in the same session when the task is complete, you can easily download the task to disk:
task.download("/path/to/save/data", session.token)


# If you restarted your python session or no longer have access to the original task object, you can also list all processed task to find your task_id:

tasks = list_task(session.token)
task = [x for x in tasks if x["task_name"] == "mesonet_aqua_download"][0]
task = Task.from_response(task)
task.download("/path/to/save/data", session.token)

# When you're done, make sure to logout of your session:
session.logout()
```
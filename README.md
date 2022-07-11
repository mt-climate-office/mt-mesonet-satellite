![MCO Logo](https://raw.githubusercontent.com/mt-climate-office/mesonet-dashboard/develop/app/assets/MCO_logo.svg)
# MT-MESONET-SATELLITE
A Python package to extract NASA satellite data at Montana Climate Office Mesonet stations using the [AppEEARS API](https://appeears.earthdatacloud.nasa.gov/api/?python) and store it in a [Neo4j](https://neo4j.com/) database. While this package was developed for the Montana Mesonet, it is flexible and could easily be applied to Mesonets in other states. The AppEEARS API requires a NASA Earthdata login ([https://urs.earthdata.nasa.gov/users/new](https://urs.earthdata.nasa.gov/users/new)). 

To use the full functionality of the package, you will need a `.env` file with the following credentials defined:
 - NEO4J_AUTH: The credentials to give the Neo4j database, formatted as {username}/{password}
 - EarthdataLogin: Your NASA Earthdata username (do not include email).
 - EarthdataPassword: Your NASA Earthdata password. 
 - Neo4jUser: The Neo4j user defined in NEO4J_AUTH
 - Neo4jURI: bolt://{container_name}, where container_name is the name of the Docker container hosting the Neo4j database.
 - Neo4jPassword: The password defined in NEO4J_AUTH. 

### Dependencies
- `git`
- `docker` (as well as the `docker-compose-plugin`)
- `poetry`

To just use the functionality of the package, you can install from GitHub using `pip` or `poetry`:

`pip install git+https://github.com/colinbrust/mt-mesonet-satellite.git@main`

or using poetry:

`poetry add git+https://github.com/colinbrust/mt-mesonet-satellite.git#main`

Below is an example of how to setup a database and perform daily downloads of data from scratch. First, stand up the Neo4j database and install the package. 

```bash
git clone https://github.com/mt-climate-office/mt-mesonet-satellite.git

cd mt-mesonet-satellite

# First, stand up the Neo4j database
docker compose up --build -d neo4j
# If you are on a Linux OS, you will need to specify your user and group ID to so you can read/write to the linked Docker volumes, where your user and group IDs are the output of id -u and id -g respectively:
# docker compose up --build -d -u 1001:1001 neo4j

# Install the package 
poetry build
poetry shell
pip install .
```

Next, run the initialize.py script to download some example data. The initialize.py script downloads VIIRS NDVI and EVI data. You can explore other datasets to download at [https://appeears.earthdatacloud.nasa.gov/products](https://appeears.earthdatacloud.nasa.gov/products):

```bash
python ./processing/update.py
```

Once the data have been uploaded into the database, you can start the Docker container automatically downloads data every day:

```bash
docker compose up --build -d update
```

This container will find the date of the last data for each product in the database, automatically download the data and upload it to the database.

from dataclasses import dataclass, field
import requests
import subprocess
from pathlib import Path
from typing import Dict, Optional, Tuple, List


@dataclass
class Session:
    """Class to store credentials necessary to use the AppEEARS API.

    To use the API, you need an NASA Earthdata login (https://urs.earthdata.nasa.gov/users/new).
    To facilitate using the AppEEARS API, it also helps to have your Earthdata credentials
    stored in a file named '~/.netrc', which is a simple text document with the following format:

    machine urs.earthdata.nasa.gov
    login yourEmail@yourDomain.com
    password yourPassword

    Attributes:
        username (Optional[str]): Earthdata username. If left as None, ~/.netrc will be used. Defaults to None.
        password (Optioanl[str]): Earthdata password. If left as None, ~/.netrc will be used. Defautls to None.
        creds (Dict[str, str]): Credentaials for a session provided after login.
        token (str): Token necessary to use AppEEARS API. Created upon login.
    """

    username: Optional[str] = None
    password: Optional[str] = None
    creds: Dict[str, str] = field(init=False)
    token: str = field(init=False)

    def __post_init__(self):
        self.creds = self._login(self.username, self.password)
        self.token = self.creds["token"]

    @staticmethod
    def _get_auth() -> Tuple[str]:
        """Get AppEEARS authentication informatiion from a ~/.netrc file.

        Returns:
            Tuple[str]: A tuple with (username, password) stored in it.
        """
        username = (
            subprocess.check_output(
                """awk '/login/ { print $2 }' ~/.netrc""", shell=True
            )
            .strip()
            .decode()
            .split("@")[0]
        )

        password = (
            subprocess.check_output(
                """awk '/password/ { print $2 }' ~/.netrc""", shell=True
            )
            .strip()
            .decode()
        )

        return username, password

    def _login(
        self, username: Optional[str] = None, password: Optional[str] = None
    ) -> Dict[str, str]:
        """Log user in to the AppEEARS API

        Args:
            username (Optional[str]): Earthdata Username. If None, ~/.netrc is used. Defaults to None.
            password (Optional[str]): Earthdata password. If None, ~/.netrc is used. Defaults to None.

        Returns:
            Dict[str, str]: A dictionary containing authentication token to use the API.
        """
        if not username or not password:
            assert (
                Path.home() / ".netrc"
            ).exists(), "If you don't provide an Earthdata Search login, you must have your login credentials stored in the ~/.netrc file."
            username, password = self._get_auth()
        response = requests.post(
            "https://appeears.earthdatacloud.nasa.gov/api/login",
            auth=(username, password),
        )

        assert (
            response.status_code == 200
        ), "Invalid status code. Please double-check your credentials."

        creds = response.json()
        return creds

    def logout(self):
        """Method to logout after a session and deactivate the token associated with the session."""
        response = requests.post(
            "https://appeears.earthdatacloud.nasa.gov/api/logout",
            headers={"Authorization": "Bearer {0}".format(self.token)},
        )

        assert (
            response.status_code == 204
        ), "Invalid HTTP response code. Please check credentials."

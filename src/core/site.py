from shareplum.site import _Site2007
import requests
from shareplum.errors import ShareplumRequestError


def get(session, url, **kwargs):
    try:
        response = session.get(url, **kwargs)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as err:
        raise ShareplumRequestError("Shareplum HTTP Get Failed", err)


def post(session, url, **kwargs):
    try:
        response = session.post(url, **kwargs)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as err:
        raise ShareplumRequestError("Shareplum HTTP Post Failed", err)


class Site365(_Site2007):
    def __init__(
        self,
        site_url,  # type: str
        auth=None,  # type: Optional[Any]
        authcookie=None,  # type: Optional[requests.cookies.RequestsCookieJar]
        verify_ssl=True,  # type: bool
        ssl_version=None,  # type: Optional[float]
        huge_tree=False,  # type: bool
        timeout=None,
    ):  # type: Optional[int]
        super().__init__(
            site_url, auth, authcookie, verify_ssl, ssl_version, huge_tree, timeout
        )

        self._session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json;odata=nometadata",
            }
        )
        self.version = "v365"

    @property
    def contextinfo(self):
        response = post(self._session, self.site_url + "/_api/contextinfo")
        return response.json()

    def upload_file(self, file_object, folder_name, file_name):
        url = (
            self.site_url
            + f"/_api/web/GetFolderByServerRelativeUrl('{folder_name}')/Files/add(url='{file_name}',overwrite=true)"
        )
        headers = {"X-RequestDigest": self.contextinfo["FormDigestValue"]}

        post(
            self._session,
            url=url,
            headers=headers,
            data=file_object,
            timeout=self.timeout,
        )

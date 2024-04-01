from io import BytesIO
from shareplum import Site, Office365
from shareplum.site import Version
from loguru import logger
import requests
from .site import Site365


class SharePoint:
    def __init__(
        self,
        sharepoint_username=None,
        sharepoint_password=None,
        sharepoint_website=None,
        sharepoint_site=None,
        sharepoint_library_name=None,
        timeout=600,
    ):
        self.sharepoint_username = sharepoint_username
        self.sharepoint_password = sharepoint_password
        self._sharepoint_website = sharepoint_website
        self._sharepoint_site = sharepoint_site
        self._sharepoint_library_name = sharepoint_library_name
        self.auth_cookie = None
        self.timeout = timeout
        self._session = requests.Session()

    def get_site(self):
        try:
            self.auth_cookie = Office365(
                self._sharepoint_website,
                username=self.sharepoint_username,
                password=self.sharepoint_password,
            ).GetCookies()

            try:
                site = Site(
                    self._sharepoint_site,
                    version=Version.v365,
                    authcookie=self.auth_cookie,
                    timeout=self.timeout,
                )
                logger.info(f"SharePoint login success")
                logger.info(f"Version:{site.version}")
                return site

            except requests.exceptions.HTTPError as sp_http_error:
                logger.info(f"SharePoint login failed：{sp_http_error}")
                response = sp_http_error.response
                logger.info(f"{response.status_code} - {response.reason}")

        except Exception as e:
            logger.info(f"Other error：{e}")

    def get_sharepoint_file_list(self, site, folder_name):
        folder_path = f"{self._sharepoint_library_name}/{folder_name}"

        # Create folder object
        folder_all = site.Folder(folder_path)

        # Get files dictionary
        file_dict = folder_all.files

        file_name_list = [file_info["Name"] for file_info in file_dict]
        logger.info(f"Get file list -> {file_name_list}")

        return file_name_list

    def get_sharepoint_data_stream(self, site, folder_name, file_name):
        folder_path = f"{self._sharepoint_library_name}/{folder_name}"
        # Create folder object
        folder_all = site.Folder(folder_path)
        # Create file object
        file_object = folder_all.get_file(file_name)
        # Byte Data->Binary Data
        data_stream = BytesIO(file_object)
        logger.info(f"get_sharepoint_data_stream() get data stream -> {file_name}")
        return file_object, data_stream

    def archive_sharepoint_file(
        self, site, file_object, archive_folder, file_name, sp_user_file_folder
    ):
        archive_folder_path = f"{self._sharepoint_library_name}/{archive_folder}"
        sp_user_file_folder_path = (
            f"{self._sharepoint_library_name}/{sp_user_file_folder}"
        )

        # Create folder object
        user_folder_object = site.Folder(sp_user_file_folder_path)

        # Archive file
        logger.info(
            f"archive_sharepoint_file() archive '{file_name}' to '{archive_folder_path}'"
        )

        site365 = Site365(
            self._sharepoint_site,
            authcookie=self.auth_cookie,
            timeout=self.timeout,
        )
        site365.upload_file(file_object, archive_folder_path, file_name)

        # Refactor function ,Because the function’s timeout is only 3
        # archive_folder_object = site.Folder(archive_folder_path)
        # archive_folder_object.upload_file(file_object, file_name)

        # Delete file of User Folder
        logger.info(
            f"delete_sharepoint_file() delete file of folder '{sp_user_file_folder}/{file_name}'"
        )
        # user_folder_object.delete_file(file_name)

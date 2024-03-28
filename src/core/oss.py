import oss2
from loguru import logger
import sys
from io import BytesIO
from oss2 import Auth
from .credentials import Temporary


class OSS:
    def __init__(
        self,
        temp_creds=None,
        oss_endpoint=None,
        oss_bucket=None,
        access_key_id=None,
        access_key_secret=None,
    ):
        self.auth = None
        self._oss_endpoint = oss_endpoint
        self._oss_bucket = oss_bucket
        self.temp_creds = temp_creds
        self.access_key_id = access_key_id
        self.access_key_secret = access_key_secret

    @staticmethod
    def _authorize(temp_creds: Temporary):
        return oss2.StsAuth(
            temp_creds.access_key_id,
            temp_creds.access_key_secret,
            temp_creds.security_token,
        )

    def get_bucket(self):
        if self.temp_creds is None:
            self.auth = Auth(self.access_key_id, self.access_key_secret)
        else:
            self.auth = OSS._authorize(self.temp_creds)
        bucket = oss2.Bucket(
            self.auth,
            self._oss_endpoint,
            self._oss_bucket,
        )
        try:
            bucket.get_bucket_acl()
            logger.info(f"OSS login success")
            return bucket
        except oss2.exceptions.NoSuchBucket:
            logger.info(f"Bucket is missing")
        except oss2.exceptions.OssError as e:
            logger.info(f"OSS login failed：{e}")

    def list_files(self, bucket, file_object, delimiter="/"):
        matching_files = []

        for obj in oss2.ObjectIterator(
            bucket=bucket, prefix=file_object, delimiter=delimiter
        ):
            if not obj.is_prefix() and obj.key.endswith(file_object):  # 根据文件名筛选
                check_file = obj.key
                matching_files.append(check_file)

        return matching_files

    def list_folders(self, bucket, folder_object, delimiter="/"):
        matching_folders = []

        for obj in oss2.ObjectIterator(
            bucket=bucket, prefix=folder_object, delimiter=delimiter
        ):
            if obj.is_prefix():
                folder = obj.key
                if folder.endswith(delimiter):
                    matching_folders.append(folder)

        return matching_folders

    def upload_with_progress(self, bucket, object_name, data_stream):
        bucket.put_object(
            object_name, data_stream.getvalue(), progress_callback=self._percentage
        )
        logger.info(f"Upload OSS -> {object_name}")

    def upload_to_oss(self, bucket, object_name, data_stream):
        bucket.put_object(object_name, data_stream.getvalue())
        logger.info(f"Upload OSS -> {object_name}")

    # Get oss data stream
    def get_oss_data_stream(self, bucket, object_name):
        object_exists = bucket.object_exists(object_name)
        if object_exists:
            logger.info(f"Object exist -> {object_name}")
        else:
            logger.info(f"Object not exist -> {object_name}")

        # Get object
        file = bucket.get_object(object_name)
        # Byte Data->Binary Data
        data_stream = BytesIO(file.read())
        return data_stream

    @staticmethod
    def _percentage(consumed_bytes, total_bytes):
        if total_bytes:
            rate = int(100 * (float(consumed_bytes) / float(total_bytes)))
            logger.info(f"----- {rate}% ------", end="\r")
            sys.stdout.flush()

from itertools import islice

import oss2
from loguru import logger
from fastapi import FastAPI, Request
from core.credentials import Temporary


api = FastAPI()


def _authorize(temp_creds: Temporary):
    return oss2.StsAuth(
        temp_creds.access_key_id,
        temp_creds.access_key_secret,
        temp_creds.security_token,
    )


@api.post("/invoke")
async def invoke(incoming_request: Request):
    # Show version
    logger.info("1.0")

    # Check incoming request header
    logger.info(f"Request header -> {incoming_request.headers}")

    # Init temporary credentials
    temp_creds = Temporary(
        access_key_id=incoming_request.headers["x-fc-access-key-id"],
        access_key_secret=incoming_request.headers["x-fc-access-key-secret"],
        security_token=incoming_request.headers["x-fc-security-token"],
    )

    async def get_bucket():
        try:
            auth = _authorize(temp_creds)
            bucket = oss2.Bucket(
                auth,
                endpoint="https://oss-cn-hangzhou-internal.aliyuncs.com",
                bucket_name="kering-cdn-prod-sharepoint",
            )
            await bucket.get_bucket_acl()
            logger.info(f"OSS login success")
            return bucket
        except oss2.exceptions.NoSuchBucket:
            logger.info(f"Bucket is missing")
        except oss2.exceptions.OssError as e:
            logger.info(f"OSS login failed：{e}")

    bucket = await get_bucket()  # 使用 await 调用异步函数

    # oss2.ObjectIterator用于遍历文件。
    for b in islice(oss2.ObjectIterator(bucket), 10):
        print(b.key)

import json

from alibaba_cloud_secretsmanager_client.secret_manager_cache_client_builder import (
    SecretManagerCacheClientBuilder,
)
from alibaba_cloud_secretsmanager_client.service.default_secret_manager_client_builder import (
    DefaultSecretManagerClientBuilder,
)
from aliyunsdkcore.auth.credentials import StsTokenCredential

from .credentials import Temporary


def _init_sts_token_credential(temp_creds: Temporary) -> StsTokenCredential:
    return StsTokenCredential(
        temp_creds.access_key_id,
        temp_creds.access_key_secret,
        temp_creds.security_token,
    )


def get_secret(temp_creds: Temporary, region: str, name: str) -> dict:
    sts_token_credentials = _init_sts_token_credential(temp_creds=temp_creds)
    secret_cache_client = SecretManagerCacheClientBuilder.new_cache_client_builder(
        DefaultSecretManagerClientBuilder.standard()
        .with_credentials(sts_token_credentials)
        .with_region(region)
        .build()
    ).build()

    secret = secret_cache_client.get_secret_info(name)  # type: ignore
    secret = secret.__dict__["secret_value"]

    return json.loads(secret)

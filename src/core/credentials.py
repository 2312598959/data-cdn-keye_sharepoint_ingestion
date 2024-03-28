from dataclasses import dataclass


@dataclass
class Temporary:
    access_key_id: str
    access_key_secret: str
    security_token: str

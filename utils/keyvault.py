from functools import lru_cache
from typing import Optional
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential, AzureCliCredential
import os


@lru_cache
def get_kv_variable(variable_name: Optional[str] = None):
    credentials = AzureCliCredential()
    # credentials = DefaultAzureCredential()
    KVUri = f"https://{os.getenv('KEY_VAULT_NAME')}.vault.azure.net"
    client = SecretClient(vault_url=KVUri, credential=credentials)
    if variable_name:
        return client.get_secret(variable_name).value

    return client
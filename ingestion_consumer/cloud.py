
import os
import json
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

secret_store = os.environ.get("SECRET_STORE")


def get_cloud_secret_selfauth(secret_name):
    '''
    Get a secret from GCS secret manager using the default credentials of the environment. 
    This will only work if you are running in an environment with a service account that has access to the secret. 
    '''
    from google.cloud import secretmanager
    try:
        # Assume you are running from a GCP service with SA that can auth itself 
        client = secretmanager.SecretManagerServiceClient() 
        # get your secret
        response = client.access_secret_version(name=secret_name)
        secret_value = response.payload.data.decode("UTF-8")
        return(secret_value)

    except Exception as e:
        logger.error(f"Failed to access secret {secret_name} with self-authentication: {e}")
        return None
    

def get_credentials_from_env():
    from dotenv import load_dotenv
    from google.oauth2 import service_account
    load_dotenv('../.env')
    env_gcs_sa = os.environ.get("GCS_SA")
    if env_gcs_sa is None:
        return None
    
    J = json.loads(env_gcs_sa)
    with open("temp_creds.json", "w") as f:
        json.dump(J, f)

    credentials = service_account.Credentials.from_service_account_file("temp_creds.json")
    return credentials


def get_secret(secret_env_var, gcs_secret_name = None, sa_creds: str = None, secret_store = secret_store): 
    from google.oauth2 import service_account
    from google.cloud import secretmanager
    load_dotenv('.env')
    secret = os.environ.get(secret_env_var)
    if secret:
        return secret
    
    elif gcs_secret_name is not None:
        gcs_secret_path = f"{secret_store}/{gcs_secret_name}"

        # Try to get it with service account authenticating itself
        secret = get_cloud_secret_selfauth(gcs_secret_path)
        if secret is not None:
            return secret

        # Try to get secret by providing service account credentials
        if sa_creds is not None:
            credentials = service_account.Credentials.from_service_account_file(sa_creds)
        else:
            credentials = get_credentials_from_env()
            
        if credentials is None:
            logger.error("No credentials available to access GCS secret")
            raise Exception("No credentials available to access GCS secret")
            
        client = secretmanager.SecretManagerServiceClient(credentials=credentials) 
        response = client.access_secret_version(name=gcs_secret_path)
        secret = response.payload.data.decode("UTF-8")
        return(secret)

    else:
        logger.error(f"Secret {secret_env_var} not found in environment variables and no GCS secret name provided")
        raise Exception(f"Secret {secret_env_var} not found in environment variables and no GCS secret name provided")


def setup_pika_client(host, port, pw, heartbeat = 60, blocked_connection_timeout = None):
    import pika
    print("getting credentials")
    credentials = pika.PlainCredentials('admin', pw)
    parameters = pika.ConnectionParameters(host, port, '/', credentials, heartbeat=heartbeat,
                                           blocked_connection_timeout = blocked_connection_timeout)

    print("connecting")
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    return(connection, channel)


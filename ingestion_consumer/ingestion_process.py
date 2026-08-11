import os
import json
import time
import logging
from importlib.metadata import distribution
from google.cloud import storage as gcs

from crucible import CrucibleClient
from crucible.utils.io import get_tz_isoformat
from crucible_ingestion import data_ingestion, set_client
from .cloud import get_secret, setup_pika_client

_GCS_PROD_BUCKET = "mf-storage-prod"

logger = logging.getLogger(__name__)


def get_ingestion_githash():
    try:
        direct_url = distribution("crucible-ingestion").read_text("direct_url.json")
        return json.loads(direct_url)["vcs_info"]["commit_id"]
    except Exception as err:
        logger.warning(f"Could not resolve crucible-ingestion githash: {err}")
        return None


ingestion_githash = get_ingestion_githash()
num_cores = os.cpu_count()

# RMQ Setup ===========================
rmq_pw = get_secret("RABBITMQ_DEFAULT_PW", "rabbitmq_default_pw/versions/1")
rmq_host = os.environ.get('RMQ_HOST')
rmq_port = os.environ.get('RMQ_PORT')
RMQ_ROUTING_SUFFIX = os.environ.get('RMQ_ROUTING_SUFFIX')

connection, channel = setup_pika_client(rmq_host, rmq_port, rmq_pw)

queues_needed = [f'ingestion-{RMQ_ROUTING_SUFFIX}', 'not-supported', f'ingestion-{RMQ_ROUTING_SUFFIX}-failed']

for q in queues_needed:
    channel.queue_declare(queue=q)

# Crucible Setup ===========================
crucible_api_url = os.environ.get('CRUCIBLE_API_URL')
crucible_apikey = get_secret("CRUCIBLE_APIKEY", "crucible_admin_apikey/versions/4")
client = CrucibleClient(api_url=crucible_api_url, api_key=crucible_apikey)
set_client(client)

# Functions ===========================
def is_file_lost(message, dataset_to_process, ch, update_status=True):
    reqid = message['reqid']
    filename = message['filename']

    # For prod bucket files, check via GCS client to bypass FUSE cache latency
    if filename.startswith(_GCS_PROD_BUCKET):
        blob_name = filename[len(_GCS_PROD_BUCKET) + 1:]
        try:
            file_exists = gcs.Client().bucket(_GCS_PROD_BUCKET).blob(blob_name).exists()
        except Exception as e:
            logger.warning(f"GCS client check failed for {blob_name}: {e}, falling back to os.path")
            file_exists = os.path.exists(dataset_to_process)
    else:
        file_exists = os.path.exists(dataset_to_process)

    if not file_exists:
        if update_status:
            client.ingestions.update(reqid, status="file not found")
        return True
    return False

def callback(ch, method, props, body):
    '''
    Expects a RMQ message with: 
    
    filename: The path in GCS to get the file that you want to ingest from
    reqid:    The ingestion request ID
    dsid:     The dataset ID that the ingestion request was made for
              and that the new data will be uploaded to

    Will skip requests for files that are: 
        - Not supported by a currently deployed ingestion class

    '''
    # get info
    message = json.loads(body.decode("utf-8").strip())
    filename = message['filename']
    filename = filename.replace('\\', '/')
    if filename.startswith('/mnt/gcs'):
        dataset_to_process = filename
    elif filename.startswith('crucible-uploads'):
        dataset_to_process = filename.replace('crucible-uploads', '/mnt/gcs', 1)
    elif filename.startswith('mf-storage-prod'):
        dataset_to_process = filename.replace('mf-storage-prod', '/mnt/gcs-prod', 1)
    else:
        logger.error(f"Unexpected filename format, cannot resolve path: {filename}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    specified_ingestor = message['ingestion_class']
    reqid = message['reqid']
    dsid = message['dsid']
    start_time = get_tz_isoformat().replace(":", "")
    logger.info(f"received message {message} .. starting processing")
    
    # update the SQL database that the ingestion has begun
    client.ingestions.update(reqid, status = "started", ingestion_githash = ingestion_githash)

    # check file found (retry up to 5 times)
    max_file_retries = 5
    for attempt in range(1, max_file_retries + 1):
        if not is_file_lost(message, dataset_to_process, ch, update_status=(attempt == max_file_retries)):
            break
        if attempt < max_file_retries:
            logger.warning(f"[x] File not found, retry {attempt}/{max_file_retries} for {body}")
            time.sleep(2 ** attempt)
        else:
            logger.error(f"[x] Received {body} but file not found after {max_file_retries} attempts")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return


    ds, ingestion_class = (None,None)
    try:
        ds, ingestion_class = data_ingestion(dataset_to_process = dataset_to_process,
                                             dsid = dsid,
                                             ingestion_class = specified_ingestor)
        
        logger.info(f"{ds=}")
        if ds is None:
            client.ingestions.update(reqid, status = "not supported", ingestion_githash = ingestion_githash)
            ch.basic_publish(exchange = '',
                            routing_key= 'not-supported',
                            body=json.dumps(message))
            logger.warning(f"[x] Received {body} and was not a supported a file type - skipping")

        else:
            client.ingestions.update(reqid,
                                     status = "complete",
                                     ingestion_githash = ingestion_githash,
                                     ingestion_class = ingestion_class)
            
            logger.info(f"[x] Received {body} and ingested with id: {ds['unique_id']}")
        
        ch.basic_ack(delivery_tag=method.delivery_tag)      
        
    except Exception as err:
        logger.error(f"[x] Received {body} but failed with error {err}")
        client.ingestions.update(reqid,
                                 "failed",
                                 ingestion_githash = ingestion_githash,
                                 ingestion_class = ingestion_class)
        ch.basic_publish(exchange = '', routing_key= f'ingestion-{RMQ_ROUTING_SUFFIX}-failed', body=json.dumps(message))
        ch.basic_ack(delivery_tag=method.delivery_tag)    
        return
        #ch.basic_nack(delivery_tag=method.delivery_tag)      


# subscribe to the queue
channel.basic_qos(prefetch_count=10)  # tune this up
channel.basic_consume(queue=f'ingestion-{RMQ_ROUTING_SUFFIX}',
                      auto_ack=False,
                      on_message_callback=callback)

# always be listening
logger.info('[*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()


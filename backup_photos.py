import base64
from datetime import datetime
import functools
import itertools
import json
import logging
import os.path
import queue
import tempfile
import threading
import time
import socket
import urllib.parse

import attr
import boto3_wasabi
from botocore.errorfactory import ClientError
import click
import pytz
import requests

from authentication import authenticate


logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)
MAX_RETRIES = 5
WAIT_SECONDS = 5


# s3 = boto3_wasabi.client('s3', aws_access_key_id=WASABI_ACCESS_KEY, aws_secret_access_key=WASABI_SECRET_KEY)
# s3_client = functools.partial(s3.put_object,
#                               Bucket=WASABI_BUCKET,
#                               ContentType='application/octet-stream')
s3_client = None


@attr.s
class Media(object):
    file_name = attr.ib()
    created_date = attr.ib()
    file_size = attr.ib()
    record = attr.ib()
    download_url = attr.ib(repr=False)
    other_media = attr.ib(default=None)

    @classmethod
    def from_record(cls, record, media_size='resOriginalRes'):
        file_name_encoded = record['fields']['filenameEnc']['value']
        file_name = base64.b64decode(file_name_encoded).decode('utf-8')

        timestamp = record['created']['timestamp'] / 1000.0
        created_date = datetime.fromtimestamp(timestamp, tz=pytz.utc)

        file_size = record['fields'][media_size]['value']['size']

        download_url = record['fields'][media_size]['value']['downloadURL']

        return cls(file_name, created_date, file_size, record, download_url)

    @classmethod
    def from_live_photo_record(cls, record):
        obj = cls.from_record(record, media_size='resOriginalVidComplRes')
        obj.file_name = obj.file_name.rsplit('.', 1)[0] + '.mov'

        return obj


@click.command()
@click.argument('directory', metavar='<directory>')
@click.option('--username',
              help='Your iCloud username or email address',
              metavar='<username>',
              prompt='iCloud username/email')
@click.option('--password',
              help='Your iCloud password '
                   '(default: use PyiCloud keyring or prompt for password)',
              metavar='<password>')
@click.option('--recent',
              help='Number of recent photos to download (default: download all photos)',
              type=click.IntRange(0))
@click.option('--until-found',
              help='Download most recently added photos until we find x number of previously downloaded consecutive photos (default: download all photos)',
              type=click.IntRange(0))
@click.option('--auto-delete',
              help='Scans the "Recently Deleted" folder and deletes any files found in there. ' + \
                   '(If you restore the photo in iCloud, it will be downloaded again.)',
              is_flag=True)
@click.option('--only-print',
              help='Only prints the filenames of all files that will be downloaded. ' + \
                '(Does not download any files.)',
              is_flag=True)
@click.option('--set-exif-datetime',
              help='Writing exif DateTimeOriginal tag from file creation date, if it\'s not exists. ',
              is_flag=True)
@click.option('--smtp-username',
              help='Your SMTP username, for sending email notifications when two-step authentication expires.',
              metavar='<smtp_username>')
@click.option('--smtp-password',
              help='Your SMTP password, for sending email notifications when two-step authentication expires.',
              metavar='<smtp_password>')
@click.option('--smtp-host',
              help='Your SMTP server host. Defaults to: smtp.gmail.com',
              metavar='<smtp_host>',
              default='smtp.gmail.com')
@click.option('--smtp-port',
              help='Your SMTP server port. Default: 587 (Gmail)',
              metavar='<smtp_port>',
              type=click.IntRange(0),
              default=587)
@click.option('--smtp-no-tls',
              help='Pass this flag to disable TLS for SMTP (TLS is required for Gmail)',
              metavar='<smtp_no_tls>',
              is_flag=True)
@click.option('--notification-email',
              help='Email address where you would like to receive email notifications. Default: SMTP username',
              metavar='<notification_email>')
def backup(directory, username, password, recent,
           until_found, auto_delete,
           only_print, set_exif_datetime,
           smtp_username, smtp_password, smtp_host, smtp_port, smtp_no_tls,
           notification_email):

    directory = os.path.normpath(directory)

    if not notification_email:
        notification_email = smtp_username

    icloud = authenticate(username, password,
        smtp_username, smtp_password, smtp_host, smtp_port, smtp_no_tls, notification_email)

    # Set up iCloud state
    base_url = icloud.webservices['ckdatabasews']['url']
    session = icloud.session
    params = {**icloud.params,
              'remapEnums': True,
              'getCurrentSyncToken': True}
    photos_endpoint = f'{base_url}/database/1/com.apple.photos.cloud/production/private'

    if check_index_state(photos_endpoint, session, params) != 'FINISHED':
        print('iCloud Photo Library not finished indexing. Please try '
              'again in a few minutes')
        exit()

    size = get_num_items(photos_endpoint, session, params)
    print("Number of photos and videos:", size)

    items = get_media(photos_endpoint, session, params,
                      query_builder=all_media_query())

    if recent is not None:
        LOGGER.info("Downloading %s recent items", recent)
        items = itertools.islice(items, recent)

    items = skip_already_saved(items, directory, until_found)
    items = make_directories(items, directory)
    items = get_all_downloadable_items(items)

    if only_print:
        print_items(items)
    else:
        download_queue = queue.Queue(maxsize=4)
        num_worker_threads = 4
        threads = []
        for i in range(num_worker_threads):
            t = threading.Thread(target=worker, args=(download_queue,))
            t.start()
            threads.append(t)

        # Download files
        for item in items:
            download_queue.put((item, directory, session))

        LOGGER.info("Waiting for all downloads to complete...")
        download_queue.join()

        # Stop workers
        for _ in range(num_worker_threads):
            download_queue.put(None)
        for t in threads:
            t.join()

        LOGGER.info("...Done downloading")

    if auto_delete:
        LOGGER.info("Deleting any files found in 'Recently Deleted'...")
        items = get_media(photos_endpoint, session, params,
                          query_builder=recently_deleted_query())

        if only_print:
            print_items(items)
        else:
            delete_files(items, directory)


def print_items(items):
    for item in items:
        LOGGER.info("%s", item.file_name)


def delete_files(items, directory):
    for media_item in items:
        download_dir = get_download_dir(media_item.created_date, directory)
        path = os.path.join(download_dir, media_item.file_name)

        if os.path.exists(path):
            LOGGER.info("Deleting %s!", path)
            os.remove(path)


def worker(download_queue):
    while True:
        item = download_queue.get()
        if item is None:
            break
        download(*item)
        download_queue.task_done()


def download(media_item, directory, session):
    download_dir = get_download_dir(media_item.created_date, directory)
    download_path = os.path.join(download_dir, media_item.file_name)

    for _ in range(MAX_RETRIES):
        try:
            response = session.get(media_item.download_url, stream=True)

            if s3_client:
                if int(response.headers['Content-Length']) > (1024 * 1024 * 50):
                    # Save big files to a temporary file so I don't eat up memory
                    with tempfile.TemporaryFile() as f:
                        LOGGER.info("Saving %s to a temporary file", download_path)
                        for chunk in response.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                f.write(chunk)
                        f.seek(0)  # Start at the beginning of the file
                        LOGGER.info("Uploading %s to S3", download_path)
                        s3_client(Key=download_path,
                                  Body=f)
                else:
                    # Small files I can read into memory
                    LOGGER.info("Uploading %s to S3", download_path)
                    s3_client(Key=download_path,
                              Body=response.content)

                return

            else:
                LOGGER.info("Downloading %s", download_path)
                with open(download_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=1024):
                        if chunk:
                            f.write(chunk)
                return

        except (requests.exceptions.ConnectionError, socket.timeout):
            LOGGER.warning('Connection failed, retrying after %d seconds...', WAIT_SECONDS)
            time.sleep(WAIT_SECONDS)
    else:
        LOGGER.error("Could not download %s!", download_path)


def get_all_downloadable_items(items):
    for item in items:
        yield item
        if item.other_media:
            yield item.other_media


def make_directories(items, directory):
    for item in items:
        download_dir = get_download_dir(item.created_date, directory)

        if not os.path.exists(download_dir):
            os.makedirs(download_dir)

        yield item


def get_download_dir(created_date, directory):
    date_path = "{:%Y/%m/%d}".format(created_date)
    return os.path.join(directory, date_path)


def skip_already_saved(items, backup_location, until_found):
    consecutive_files_found = 0

    for item in items:
        saved = already_saved(item, backup_location)

        if saved and item.other_media:
            saved = already_saved(item.other_media, backup_location)

        if saved:
            download_dir = get_download_dir(item.created_date, backup_location)
            download_path = os.path.join(download_dir, item.file_name)
            LOGGER.info("Skipping %s", download_path)

            consecutive_files_found += 1

            if until_found is not None and consecutive_files_found >= until_found:
                LOGGER.warning("Found %s consecutive files. Stopping!",
                               consecutive_files_found)
                break
        else:
            consecutive_files_found = 0
            yield item


def already_saved(item, backup_location):
    download_dir = get_download_dir(item.created_date, backup_location)
    download_path = os.path.join(download_dir, item.file_name)
    expected_size = item.file_size

    if s3_client:
        try:
            response = s3.head_object(Bucket=WASABI_BUCKET, Key=download_path)
            actual_size = response['ContentLength']
            if actual_size != expected_size:
                LOGGER.warning("Re-downloading %s because sizes were different: %s & %s",
                               download_path,
                               actual_size,
                               expected_size)
                return False
            else:
                return True
        except ClientError as e:
            return False
    else:
        LOGGER.debug("Looking to see if %s exists", download_path)
        if not os.path.isfile(download_path):
            return False

        try:
            actual_size = os.path.getsize(download_path)
            LOGGER.debug("Checking file size: %s ≟ %s", actual_size, expected_size)
            if actual_size != expected_size:
                LOGGER.warning("Re-downloading %s because sizes were different: %s & %s",
                               download_path,
                               actual_size,
                               expected_size)
                return False
            else:
                return True
        except OSError:
            LOGGER.exception("An error occurred while getting size of file")
            return False


def all_media_query():
    return functools.partial(build_query,
                             list_type='CPLAssetAndMasterByAddedDate',
                             direction='ASCENDING')


def recently_deleted_query():
    return functools.partial(build_query,
                             list_type='CPLAssetAndMasterDeletedByExpungedDate',
                             direction='ASCENDING')


def build_query(list_type, direction, offset, page_size=100):
    page_size = page_size * 2

    return {
        'query': {
            'filterBy': [
                {'fieldName': 'startRank', 'fieldValue':
                    {'type': 'INT64', 'value': offset},
                    'comparator': 'EQUALS'},
                {'fieldName': 'direction', 'fieldValue':
                    {'type': 'STRING', 'value': direction},
                    'comparator': 'EQUALS'}
            ],
            'recordType': list_type
        },
        'resultsLimit': page_size,
        'desiredKeys': ['resOriginalRes', 'resOriginalVidComplRes', 'filenameEnc',
                        'masterRef'],
        'zoneID': {'zoneName': 'PrimarySync'}
    }


def get_media(endpoint, session, params, query_builder):
    url = f'{endpoint}/records/query?{urllib.parse.urlencode(params)}'
    offset = 0

    while True:
        query = query_builder(offset=offset)

        request = session.post(url,
                               data=json.dumps(query),
                               headers={'Content-type': 'text/plain'})
        response = request.json()
        records = response['records']
        master_records = [record for record in records if record['recordType'] == 'CPLMaster']
        LOGGER.debug("Received %s master records", len(master_records))

        if len(master_records) == 0:
            LOGGER.debug("No more master records. Stopping!")
            break

        offset += len(master_records)

        for record in master_records:
            media = Media.from_record(record)

            # Check for Live Photo video
            if 'resOriginalVidComplRes' in record['fields']:
                media.other_media = Media.from_live_photo_record(record)

            LOGGER.debug("Yielding %s", media)
            yield media


def get_num_items(endpoint, session, params):
    url = f'{endpoint}/internal/records/query/batch?{urllib.parse.urlencode(params)}'
    query = {
        'batch': [{
            'resultsLimit': 1,
            'query': {
                'filterBy': {
                    'fieldName': 'indexCountID',
                    'fieldValue': {
                        'type': 'STRING_LIST',
                        'value': ['CPLAssetByAddedDate']
                    },
                    'comparator': 'IN'
                },
                'recordType': 'HyperionIndexCountLookup'
            },
            'zoneWide': True,
            'zoneID': {
                'zoneName': 'PrimarySync'
            }
        }]
    }
    request = session.post(url,
                           data=json.dumps(query),
                           headers={'Content-type': 'text/plain'})
    response = request.json()
    length = (response["batch"][0]["records"][0]["fields"]
                 ["itemCount"]["value"])

    return length


def check_index_state(endpoint, session, params):
    url = f'{endpoint}/records/query?{urllib.parse.urlencode(params)}'
    json_data = ('{"query":{"recordType":"CheckIndexingState"},'
                 '"zoneID":{"zoneName":"PrimarySync"}}')
    request = session.post(url,
                           data=json_data,
                           headers={'Content-type': 'text/plain'})
    response = request.json()
    indexing_state = response['records'][0]['fields']['state']['value']
    return indexing_state




if __name__ == '__main__':
    backup()

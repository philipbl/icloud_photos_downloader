import base64
from datetime import datetime
import functools
import itertools
import json
import logging
import os.path
import queue
import threading
import time
import socket
import urllib.parse

import attr
import click
import pytz
import requests

from authentication import authenticate
from backends import filesystem
from backends import wasabi as wasabi_backend


logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)
MAX_RETRIES = 5
WAIT_SECONDS = 5


@attr.s
class Media(object):
    file_name = attr.ib()
    created_date = attr.ib()
    file_size = attr.ib()
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

        return cls(file_name, created_date, file_size, download_url)

    @classmethod
    def from_live_photo_record(cls, record):
        obj = cls.from_record(record, media_size='resOriginalVidComplRes')
        obj.file_name = obj.file_name.rsplit('.', 1)[0] + '.mov'

        return obj

@click.group()
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
@click.pass_context
def cli(ctx, username, password, smtp_username, smtp_password, smtp_host, 
        smtp_port, smtp_no_tls, notification_email, **kwargs):

    if not notification_email:
        notification_email = smtp_username

    icloud = authenticate(username, 
                          password,
                          smtp_username, 
                          smtp_password, 
                          smtp_host, 
                          smtp_port, 
                          smtp_no_tls, 
                          notification_email)

    # Set up iCloud state
    base_url = icloud.webservices['ckdatabasews']['url']
    session = icloud.session
    params = {**icloud.params,
              'remapEnums': True,
              'getCurrentSyncToken': True}
    photos_endpoint = f'{base_url}/database/1/com.apple.photos.cloud/production/private'

    def post(url, **kwargs):
        return session.post(f'{photos_endpoint}/{url}',
                            params=params,
                            **kwargs)
        

    if check_index_state(post) != 'finished':
        print('iCloud Photo Library not finished indexing. Please try '
              'again in a few minutes')
        exit()

    # Pass information to other commands
    ctx.obj = kwargs
    ctx.obj['session'] = session
    ctx.obj['post'] = post


@cli.command()
@click.argument('access_key', metavar='<key>')
@click.argument('secret_key', metavar='<key>')
@click.argument('bucket_name', metavar='<name>')
@click.pass_context
def wasabi(ctx, access_key, secret_key, bucket_name):
    backend = wasabi_backend.AwsS3(access_key, secret_key, bucket_name)
    backup(backend, **ctx.obj)


@cli.command()
@click.argument('directory', type=click.Path(exists=True), metavar='<directory>')
@click.pass_context
def file(ctx, directory):
    directory = os.path.normpath(directory)
    backend = filesystem.FileSystem(directory)
    backup(backend, **ctx.obj)


def backup(backend, post, session, recent, until_found, only_print, auto_delete):
    # size = get_num_items(photos_endpoint, session, params)
    # print("Number of photos and videos:", size)

    items = get_media(post, query_builder=all_media_query())

    if recent is not None:
        LOGGER.info("Downloading %s recent items", recent)
        items = itertools.islice(items, recent)

    items = skip_already_saved(backend, items, until_found)
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
            download_queue.put((item, session))

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
        items = get_media(post, query_builder=recently_deleted_query())

        if only_print:
            print_items(items)
        else:
            delete_files(items)


def print_items(items):
    for item in items:
        LOGGER.info("%s", item.file_name)


def delete_files(items):
    for item in items:
        backend.delete_file(item)
        
        if item.other_media:
            backend.delete_file(item.other_media)
        

def worker(download_queue):
    while True:
        item = download_queue.get()
        if item is None:
            break
        download(*item)
        download_queue.task_done()


def download(media_item, session):
    for _ in range(MAX_RETRIES):
        try:
            response = session.get(media_item.download_url, stream=True)
            backend.save_file(media_item, response)
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

def get_download_dir(created_date, directory):
    date_path = "{:%Y/%m/%d}".format(created_date)
    return os.path.join(directory, date_path)


def skip_already_saved(backend, items, until_found):
    consecutive_files_found = 0

    for item in items:
        saved = backend.already_saved(item)

        if saved and item.other_media:
            saved = backend.already_saved(item.other_media)

        if saved:
            LOGGER.info("Skipping %s", item)
            consecutive_files_found += 1

            if until_found is not None and consecutive_files_found >= until_found:
                LOGGER.warning("Found %s consecutive files. Stopping!",
                               consecutive_files_found)
                break
        else:
            consecutive_files_found = 0
            yield item

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


def get_media(post, query_builder):
    url = 'records/query'
    offset = 0

    while True:
        query = query_builder(offset=offset)

        request = post(url,
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


def get_num_items(post):
    url = 'internal/records/query/batch'
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
    request = post(url,
                   data=json.dumps(query),
                   headers={'Content-type': 'text/plain'})
    response = request.json()
    length = (response["batch"][0]["records"][0]["fields"]
                 ["itemCount"]["value"])

    return length


def check_index_state(post):
    url = 'records/query'
    json_data = ('{"query":{"recordType":"CheckIndexingState"},'
                 '"zoneID":{"zoneName":"PrimarySync"}}')
    request = post(url,
                   data=json_data,
                   headers={'Content-type': 'text/plain'})
    response = request.json()
    indexing_state = response['records'][0]['fields']['state']['value']
    return indexing_state



if __name__ == '__main__':
    cli(obj={}) 

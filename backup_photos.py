import base64
from datetime import datetime
import itertools
import json
import logging
import os.path
import queue
import threading
import urllib.parse

import attr
import click
import pytz

from authentication import authenticate


logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


@attr.s
class Media(object):
    file_name = attr.ib()
    created_date = attr.ib()
    file_size = attr.ib()
    download_url = attr.ib(repr=False)

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



CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])
@click.command(context_settings=CONTEXT_SETTINGS, options_metavar='<options>')
@click.argument('directory', type=click.Path(exists=True), metavar='<directory>')
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
@click.option('--only-print-filenames',
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
           only_print_filenames, set_exif_datetime,
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

    items = get_media(photos_endpoint, session, params)

    if recent is not None:
        LOGGER.info("Downloading %s recent items", recent)
        items = itertools.islice(items, recent)

    items = skip_already_saved(items, directory, until_found)
    items = make_directories(items, directory)

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

    response = session.get(media_item.download_url, stream=True)

    LOGGER.info("Downloading %s", download_path)
    with open(download_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)


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
        download_dir = get_download_dir(item.created_date, backup_location)
        download_path = os.path.join(download_dir, item.file_name)

        if already_saved(download_path, item.file_size):
            LOGGER.info("Skipping %s", download_path)
            consecutive_files_found += 1

            if until_found is not None and consecutive_files_found >= until_found:
                LOGGER.warning("Found %s consecutive files. Stopping!",
                               consecutive_files_found)
                break
        else:
            consecutive_files_found = 0
            yield item


def already_saved(download_path, expected_size):
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


def get_media(endpoint, session, params, page_size=100):
    url = f'{endpoint}/records/query?{urllib.parse.urlencode(params)}'

    offset = 0
    direction = 'ASCENDING'  # Also can be DESCENDING
    page_size = page_size * 2

    while True:
        query = {
            'query': {
                'filterBy': [
                    {'fieldName': 'startRank', 'fieldValue':
                        {'type': 'INT64', 'value': offset},
                        'comparator': 'EQUALS'},
                    {'fieldName': 'direction', 'fieldValue':
                        {'type': 'STRING', 'value': direction},
                        'comparator': 'EQUALS'}
                ],
                'recordType': 'CPLAssetAndMasterByAddedDate'
            },
            'resultsLimit': page_size,
            'desiredKeys': ['resOriginalRes', 'resOriginalVidComplRes', 'filenameEnc',
                            'masterRef'],
            'zoneID': {'zoneName': 'PrimarySync'}
        }

        request = session.post(url,
                               data=json.dumps(query),
                               headers={'Content-type': 'text/plain'})
        response = request.json()
        records = response['records']
        master_records = [record for record in records if record['recordType'] == 'CPLMaster']
        LOGGER.info("Received %s master records", len(master_records))

        if len(master_records) == 0:
            LOGGER.info("No more master records. Stopping!")
            break

        offset += len(master_records)

        for record in master_records:
            media = Media.from_record(record)
            LOGGER.info("Yielding %s", media)
            yield media

            # Check for Live Photo video
            if 'resOriginalVidComplRes' in record['fields']:
                media = Media.from_live_photo_record(record)
                LOGGER.info("Yielding %s", media)
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

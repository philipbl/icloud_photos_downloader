import logging
import os
from pathlib import Path, PurePath


LOGGER = logging.getLogger(__name__)


class FileSystem():
    def __init__(self, directory):
        self.backup_location = directory

    def already_saved(self, item):
        download_path = self.get_download_path(item)
        expected_size = item.file_size

        LOGGER.debug("Looking to see if %s exists", download_path)
        if not download_path.is_file():
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

    def save_file(self, item, response):
        """ Must be thread safe """
        download_path = self.get_download_path(item)
        download_path.parent.mkdir(parents=True, exist_ok=True)

        LOGGER.info("Downloading %s", download_path)
        with open(download_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)

    def delete_file(self, item):
        path = self.get_download_path(item)
        if os.path.exists(path):
            LOGGER.info("Deleting %s!", path)
            os.remove(path)

    def get_download_path(self, item):
        date_path = "{:%Y/%m/%d}".format(item.created_date)
        return Path(self.backup_location, date_path, item.file_name)


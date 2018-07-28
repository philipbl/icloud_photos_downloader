import logger
import os


class FileSystem():
    def already_saved(self, path, expected_size):
        LOGGER.debug("Looking to see if %s exists", path)
        if not os.path.isfile(path):
            return False

        try:
            actual_size = os.path.getsize(path)
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
        
    def make_directory(self, directory):
        if not os.path.exists(directory):
            os.makedirs(directory)

        
    def save_file(self, path, data):
        LOGGER.info("Downloading %s", download_path)
        with open(path, 'wb') as f:
            for chunk in data.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        
    def delete_file(self, path):
        if os.path.exists(path):
            LOGGER.info("Deleting %s!", path)
            os.remove(path)

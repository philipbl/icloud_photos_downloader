import logger
import os


class FileSystem():
    def __init__(self):
        pass
        
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

    # Must be thread safe    
    def save_file(self, path, data):
        # TODO: Get directory
        if not os.path.exists(directory):
            try:
                os.makedirs(directory)
            except:
                # TODO: Find correct exception
                # Necessary in case a different thread created it between the 
                # check and creation
                pass
            
        LOGGER.info("Downloading %s", download_path)
        with open(path, 'wb') as f:
            for chunk in data.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        
    def delete_file(self, path):
        if os.path.exists(path):
            LOGGER.info("Deleting %s!", path)
            os.remove(path)

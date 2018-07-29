import logging

import boto3

# s3_client = functools.partial(s3.put_object,
#                               Bucket=WASABI_BUCKET,
#                               ContentType='application/octet-stream')

class AwsS3():
    def __init__(self, access_key, secret_key, bucket_name):
        self.client = boto3.client('s3', 
                                   endpoint_url='https://s3.wasabisys.com',
                                   aws_access_key_id=access_key, 
                                   aws_secret_access_key=secret_key)

    def already_saved(self, expected_size):
        try:
            response = self.client.head_object(Bucket=WASABI_BUCKET, Key=download_path)
            actual_size = response['ContentLength']
            if actual_size != expected_size:
                LOGGER.warning("Re-downloading %s because sizes were different: %s & %s",
                               path,
                               actual_size,
                               expected_size)
                return False
            else:
                return True
        except ClientError as e:
            return False
        
    def save_file(self, data):
        if int(data.headers['Content-Length']) > (1024 * 1024 * 50):
            # Save big files to a temporary file so I don't eat up memory
            with tempfile.TemporaryFile() as f:
                LOGGER.info("Saving %s to a temporary file", download_path)
                for chunk in data.iter_content(chunk_size=1024 * 1024):
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
        
    def delete_file(self, item):
        pass

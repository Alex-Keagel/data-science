import json
import boto


class SecretsClient(object):
    def __init__(self, host, bucket):
        self.host = host
        self.bucket = bucket

    def get_secrets_from_file(self, file_name):
        return json.loads(boto.connect_s3(host=self.host)
                          .get_bucket(self.bucket)
                          .get_key(file_name).get_contents_as_string())

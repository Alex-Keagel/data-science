import boto

DEFAULT_REGION = 'us-east-1'
DEFAULT_BUCKET = 'eds-atlas-telaviv-nonprod'
DATA_PATH = 'DevSandbox/flatten_events_partitioned_by_date_test/'


def get_latest_partition(bucket_name, path):
    conn = boto.s3.connect_to_region(DEFAULT_REGION)
    bucket = conn.get_bucket(bucket_name)
    folders = []
    for key in list(bucket.list(prefix=path, delimiter="/")):
        if key.name.endswith('/'):
            folders.append(key.name)
    return max(folders)


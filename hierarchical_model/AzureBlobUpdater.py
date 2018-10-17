import requests
import json
import boto
import pickle


azureStorageKeys = json.loads(boto.connect_s3(host='s3.amazonaws.com').get_bucket('eds-atlas-telaviv-nonprod').get_key('recommendations-keys-telaviv/azure-storage-keys.json').get_contents_as_string(encoding='utf-8'))
urlSharedBaseAddress = 'https://home1recommendations.blob.core.windows.net/models/{model}_{modelVersion}.pkl{sasToken}'
sasSharedToken = azureStorageKeys["home1recommendations_readwrite"]
headers = {'x-ms-blob-type': 'BlockBlob'}


def blobSharedForModel(superDict, model, modelVersion):
    try:
        url = urlSharedBaseAddress.format(model=model, modelVersion=modelVersion, sasToken=sasSharedToken)
        response = requests.put(url, headers=headers, data=pickle.dumps(superDict))
        print(response.status_code)
        response.raise_for_status()
        return [(1, 1)]
    except Exception as ex:
        return [(0, 1)]

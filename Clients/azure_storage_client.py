import requests
import json


class AzureTable:
    def __init__(self, storage_account_name, table_name, table_sas, is_emulated, logger):
        self.storage_account_name = storage_account_name
        self.table_name = table_name
        self.table_sas = table_sas
        self.is_emulated = is_emulated
        self.logger = logger
        self.headers = {'Accept-Encoding': 'identity',
                        'Accept': 'application/json;odata=minimalmetadata',
                        'Content-Type': 'application/json',
                        'x-ms-version': '2016-05-31'}

    def insert_or_replace(self, entity):
        success = False
        try:
            url = self._build_table_url_(
                entity,
                self.storage_account_name,
                self.table_name,
                self.table_sas,
                self.is_emulated)
            r = requests.put(url=url, data=json.dumps(entity), headers=self.headers)
            r.raise_for_status()
            success = True
        except Exception as ex:
            self.logger.error('Error occurred "insert or replace" ' + self.table_name, ex)
            self.insert_or_replace(entity)
        finally:
            return success

    @staticmethod
    def _build_table_url_(entity, storage_account_name, table_name, table_sas, is_emulated):
        if is_emulated:
            return "http://127.0.0.1:10002/devstoreaccount1/{0}(PartitionKey='{1}',RowKey='{2}'){3}".format(
                table_name,
                entity['PartitionKey'],
                entity['RowKey'],
                table_sas)
        else:
            return "https://{0}.table.core.windows.net/{1}(PartitionKey='{2}',RowKey='{3}'){4}".format(
                storage_account_name.lower(),
                table_name,
                entity['PartitionKey'],
                entity['RowKey'],
                table_sas
            )

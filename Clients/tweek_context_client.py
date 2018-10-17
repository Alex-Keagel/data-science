import requests
import json


class TweekContextClient:
    headers = {'Content-Type': 'application/json'}
    tweek_api_playground_url = "https://api.playground.tweek.host/api/v1/context/device/{deviceId}"
    tweek_api_url = "https://tweek.mysoluto.com/api/v1/context/device/{deviceId}"

    def __init__(self, active_directory_client, is_playground, logger):
        self.active_directory_client = active_directory_client
        self.is_playground = is_playground
        self.url_per_device_base_address = self.tweek_api_playground_url if is_playground else self.tweek_api_url
        self.logger = logger
        self.token = self.active_directory_client.get_token()

    def update_device_context(self, device_id, payload):
        success = False
        if type(payload) is not dict:
            raise TypeError("payload should be a dictionary")
        if not self.is_playground:
            self.headers['Authorization'] = 'Bearer ' + self.token["accessToken"]
        try:
            url = self.url_per_device_base_address.format(deviceId=device_id)
            payload = payload
            response = requests.post(url, data=json.dumps(payload), headers=self.headers)
            response.raise_for_status()
            success = True
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                self.token = self.active_directory_client.get_token()
                self.update_device_context(device_id, payload)
                self.logger.error('HTTPError occurred while updating device context on Tweek', e)
        except Exception as ex:
            self.logger.error('Error occurred while updating device context on Tweek', ex)
        finally:
            return success

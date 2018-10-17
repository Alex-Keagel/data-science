import adal


class ActiveDirectoryClient(object):
    ad_authority_url = 'https://login.microsoftonline.com/8394d86a-a24c-4d2c-ad99-57a3d0bb7d89'
    ad_resource = 'https://tweek.mysoluto.com'

    def __init__(self, active_directory_keys):
        self.ad_client_id = active_directory_keys["client-id"]
        self.ad_client_secret = active_directory_keys["client-secret"]

    def get_token(self):
        context = adal.AuthenticationContext(self.ad_authority_url, validate_authority=True)
        return context.acquire_token_with_client_credentials(self.ad_resource, self.ad_client_id, self.ad_client_secret)

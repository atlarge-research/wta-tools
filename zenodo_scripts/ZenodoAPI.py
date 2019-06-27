import json

import requests


class ZenodoAPI(object):

    def __init__(self, base_url, api_key):
        # Send the API token as a query parameter with each request.
        self.base_url = base_url
        self.api_key = api_key

        self.headers = {"Content-Type": "application/json"}

    def check_connection(self):
        return requests.get('{}/deposit/depositions'.format(self.base_url),
                            params={'access_token': self.api_key})

    def create_empty_repository(self):
        return requests.post('{}/deposit/depositions'.format(self.base_url),
                             params={'access_token': self.api_key}, json={},
                             headers=self.headers)

    def publish_repository(self, deposition_id):
        """Publish a repository"""
        return requests.post('{}/deposit/depositions/{}/actions/publish'.format(self.base_url, deposition_id),
                             params={'access_token': self.api_key})

    def add_file_to_repository(self, deposition_id, data, files):
        """Upload files to a repository
            data: a dictionary with key 'filename': <file name>
            files: a dictionary with key 'file': <file object from e.g. open()>
        """
        return requests.post('{}/deposit/depositions/{}/files'.format(self.base_url, deposition_id),
                             params={'access_token': self.api_key}, data=data,
                             files=files)

    def delete_file_from_repository(self, deposition_id, file_id):
        return requests.delete('{}/api/deposit/depositions/{}/files/{}'.format(self.base_url, deposition_id, file_id),
                             params={'access_token': self.api_key})

    def add_metadata_to_repository(self, deposition_id, data):
        """Updates the metadata of a repository"""
        return requests.put('{}/deposit/depositions/{}'.format(self.base_url, deposition_id),
                            params={'access_token': self.api_key}, data=json.dumps(data),
                            headers=self.headers)

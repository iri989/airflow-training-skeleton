from os import path
from airflow.hooks.base_hook import BaseHook
from requests import Session
class RequestsHook(BaseHook):
    """Airflow Hook for Requests."""
    def __init__(self, conn_id: str):
        super().__init__(source=None)
        self._conn_id = conn_id
        self._base_url = None
        self._conn = None
    def get_conn(self):
        if self._conn is None:
            connection = self.get_connection(self._conn_id)
            self._base_url = connection.host
            self._conn = Session()
        return self._conn
    def request(self, method: str, url: str, params: dict = None, data: dict = None, json: bool = True):
        """"""
        request_client = self.get_conn()
        if params:
            query_params = '?' + '&'.join([f'{key}={value}' for key, value in params.items()])
        full_url = path.join(self._base_url, url, query_params)
        response = request_client.request(
            method=method,
            url=full_url,
            data=data,
            json=json
        )
        return response
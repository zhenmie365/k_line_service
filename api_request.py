import json
import logging
import pandas as pd

logger = logging.getLogger("api_request")
logger.setLevel(logging.DEBUG)

try:
    from urllib.request import urlopen, Request
except ImportError:
    from urllib2 import urlopen, Request

class api_http():
    def __init__(self, http_url, token, protocol):
        self.__http_url = http_url
        self.__token = token
        self.__protocol = protocol

    def req_http_api(self, req_params):
        req = Request(
            self.__http_url,
            json.dumps(req_params).encode('utf-8'),
            method='POST'
        )

        res = urlopen(req)
        result = json.loads(res.read().decode('utf-8'))

        if result['code'] != 0:
            raise Exception(result['msg'])

        return result['data']

    def req_zmq_api(req_params):
        print("pending")
        pass


    def query(self, api_name, fields='', **kwargs):
        req_params = {
            'api_name': api_name,
            'token': self.__token,
            'params': kwargs,
            'fields': fields
        }

        if self.__protocol == 'tcp':
            data = self.req_zmq_api(req_params)
        elif self.__protocol == 'http':
            data = self.req_http_api(req_params)
        else:
            raise Warning('{} is unsupported protocol'.format(self.__protocol))

        columns = data['fields']
        items = data['items']

        return pd.DataFrame(items, columns=columns)
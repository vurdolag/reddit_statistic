from cryptography.fernet import Fernet
from aiohttp import web
try:
    import ujson as json
except ImportError:
    import json

from util import logs


class Data:
    def __init__(self, data_json, query, request: web.Request = None):
        self.data_json = data_json
        self.query = query
        self.request: web.Request = request

    def __getattr__(self, item):
        res = self.data_json.get(item)
        if not res:
            res = self.query.get(item, '')
        return res


def get_data(func):
    async def wrapper(self, request: web.Request):
        #t1 = time()
        try:
            data = await request.content.read()
            data_json = {}
            if data:
                data = self.decript(data)
                data_json = json.loads(data.decode('utf-8'))

        except Exception as ex:
            logs.server_error()
            return self.error(f'error pars json data: {ex}')

        query = request.rel_url.query

        if self.key_api == query.get('key', ''):
            data = Data(data_json, query, request)
            try:
                res = await func(self, data)
                #print(time()-t1)
                return res
            except Exception as ex:
                logs.server_error()
                return self.error(f'Server error: {ex}')
        else:
            return self.error('error api key')

    return wrapper


class Server:
    __slots__ = 'key_api', 'app', 'enc_key', 'temp', 'cipher'

    def __init__(self):
        self.enc_key = b'9eBV0yrDK_gv-70_c77edd4zAPrMgnai9lKmFxgvTG0='
        self.key_api = 'uw74eg45yiuw6fhis6dvbfvbsshd4'
        self.app = self.app_create()
        self.cipher = Fernet(self.enc_key)

        self.temp = {}

    def encrypt(self, data: bytes) -> bytes:
        return self.cipher.encrypt(data)

    def decript(self, data: bytes) -> bytes:
        return self.cipher.decrypt(data)

    def toencjson(self, obj):
        try:
            return self.encrypt(json.dumps(obj).encode()).decode()
        except Exception as ex:
            return self.encrypt(str(ex).encode()).decode()

    def __getattr__(self, item):
        return lambda obj: web.json_response({item: self.toencjson(obj)})

    def app_create(self):
        app = web.Application()
        app.add_routes(self.routes())
        return app

    def routes(self):
        return [
            web.post('/action', self.do_action),
        ]

    def run(self):
        web.run_app(self.app, port=7070)

    def add(self, *args):
        [self.app.on_startup.append(startup) for startup in args]
        return self





from datetime import datetime
from functools import partialmethod
from json import JSONDecodeError
from urllib.parse import urljoin

from requests import Session
from tqdm import tqdm

from utils.list_to_dict import list_to_dict
from .base import Base

__all__ = [
    'IAImportExport',
]

_DATETIME_SIMPLE_FORMAT = '%Y-%m-%dT%H:%M:%S'


class IAImportExport(Base):

    def __init__(self, login, password, base_url, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._base_url = base_url
        self._login = login
        self._password = password

        self._session = Session()
        self._session.verify = False

        self.cache = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._session.close()

    def _make_url(self, uri):
        return urljoin(self._base_url, uri)

    @staticmethod
    def _make_entity_name(filename, timestamp=datetime.now()):
        return '({}) {}'.format(
            timestamp.strftime(_DATETIME_SIMPLE_FORMAT),
            filename
        )

    def _get_from_rest_collection(self, table):
        if table not in self.cache:
            self.cache[table] = []
            self._perform_login()
            counter = 0
            step = 100000
            if table == 'specification_item':
                order_by = '&order_by=parent_id&order_by=child_id'
            elif table == 'operation_profession':
                order_by = '&order_by=operation_id&order_by=profession_id'
            else:
                order_by = '&order_by=id'
            pbar = tqdm(desc=f'Получение данных из таблицы {table}')
            while True:
                temp = self._perform_get(
                    f'rest/collection/{table}'
                    f'?start={counter}'
                    f'&stop={counter + step}'
                    f'{order_by}'
                )
                pbar.total = temp['meta']['count']
                counter += step
                pbar.update(min(
                    step,
                    temp['meta']['count'] - (counter - step)
                ))
                if table not in temp:
                    break
                self.cache[table] += temp[table]
                if counter >= temp['meta']['count']:
                    break
        return self.cache[table]

    def _get_main_session(self):
        return self._perform_get('action/primary_simulation_session')['data']

    def _perform_json_request(self, http_method, uri, **kwargs):
        url = self._make_url(uri)
        logger = self._logger

        logger.debug('Выполнение {} запроса '
                     'по ссылке {!r}.'.format(http_method, url))

        logger.debug('Отправляемые данные: {!r}.'.format(kwargs))

        response = self._session.request(http_method,
                                         url=url,
                                         **kwargs)
        try:
            response_json = response.json()
        except JSONDecodeError:
            logger.error('Получен ответ на {} запрос по ссылке {!r}: '
                         '{!r}'.format(http_method, url, response))
            raise JSONDecodeError

        logger.debug('Получен ответ на {} запрос по ссылке {!r}: '
                     '{!r}'.format(http_method, url, response_json))
        return response_json

    _perform_get = partialmethod(_perform_json_request, 'GET')

    def _perform_post(self, uri, data):
        return self._perform_json_request('POST', uri, json=data)

    def _perform_put(self, uri, data):
        return self._perform_json_request('PUT', uri, json=data)

    def _perform_action(self, uri_part, **data):
        return self._perform_post(
            '/action/{}'.format(uri_part),
            data=data
        )

    def _perform_login(self):
        return self._perform_action(
            'login',
            data={
                'login': self._login,
                'password': self._password
            },
            action='login'
        )['data']

    def get_phase_with_operation_id(self, operation_id):

        if 'phase_identity' in self.cache:
            return self.cache['phase_identity'].get(operation_id)

        self.cache['phase_identity'] = {}

        entity_routes_dict = list_to_dict(
            self._get_from_rest_collection('entity_route')
        )

        operation_dict = list_to_dict(sorted(
            self._get_from_rest_collection('operation'),
            key=lambda k: k['nop']
        ))

        entity_routes_phases_dict = list_to_dict(
            self._get_from_rest_collection('entity_route_phase')
        )

        for op_id, operation in operation_dict.items():
            if operation['entity_route_phase_id'] is None:
                if '(' not in operation['identity']:
                    tqdm.write(f'Не найдена фаза для '
                               f'операции {operation["identity"]}')
                continue
            self.cache['phase_identity'][op_id] = entity_routes_phases_dict[
                    operation['entity_route_phase_id']
                ]['identity']

        return self.cache['phase_identity'].get(operation_id)

    def get_first_phase_operation(self, phase_identity):

        if 'first_operation_identity' in self.cache:
            if phase_identity not in self.cache['first_operation_identity']:
                tqdm.write(f'Не нашли первую операцию '
                           f'маршрута для {phase_identity}')
                print(self.cache['first_operation_identity'])
            return self.cache['first_operation_identity'].get(phase_identity)

        self.cache['first_operation_identity'] = {}

        operation_dict = list_to_dict(sorted(
            self._get_from_rest_collection('operation'),
            key=lambda k: k['nop']
        ))

        entity_routes_phases_dict = list_to_dict(
            self._get_from_rest_collection('entity_route_phase')
        )

        for op_id, operation in operation_dict.items():
            if operation['entity_route_phase_id'] is None:
                if '(' not in operation['identity']:
                    tqdm.write(f'Не найдена фаза для '
                               f'операции {operation["identity"]}')
                continue
            cur_phase_identity = entity_routes_phases_dict[
                operation['entity_route_phase_id']
            ]['identity']
            if cur_phase_identity not in self.cache['first_operation_identity']:
                self.cache['first_operation_identity'][cur_phase_identity] = \
                    f"{cur_phase_identity}_{operation['nop']}"
            if f"{cur_phase_identity}_{operation['nop']}" < \
                    self.cache['first_operation_identity'][cur_phase_identity]:
                self.cache['first_operation_identity'][
                    cur_phase_identity
                ] = f"{cur_phase_identity}_{operation['nop']}"

        return self.cache['first_operation_identity'].get(phase_identity)

    def get_last_phase_operation(self, phase_identity):

        if 'last_operation_identity' in self.cache:
            if phase_identity not in self.cache['last_operation_identity']:
                tqdm.write(f'Не нашли последнюю операцию '
                           f'маршрута для {phase_identity}')
                print(self.cache['last_operation_identity'])
            return self.cache['last_operation_identity'].get(phase_identity)

        self.cache['last_operation_identity'] = {}

        operation_dict = list_to_dict(sorted(
            self._get_from_rest_collection('operation'),
            key=lambda k: k['nop']
        ))

        entity_routes_phases_dict = list_to_dict(
            self._get_from_rest_collection('entity_route_phase')
        )

        for op_id, operation in operation_dict.items():
            if operation['entity_route_phase_id'] is None:
                if '(' not in operation['identity']:
                    tqdm.write(f'Не найдена фаза для '
                               f'операции {operation["identity"]}')
                continue
            cur_phase_identity = entity_routes_phases_dict[
                operation['entity_route_phase_id']
            ]['identity']
            if cur_phase_identity not in self.cache['last_operation_identity']:
                self.cache['last_operation_identity'][cur_phase_identity] = \
                    f"{cur_phase_identity}_{operation['nop']}"
            if f"{cur_phase_identity}_{operation['nop']}" > \
                    self.cache['last_operation_identity'][cur_phase_identity]:
                self.cache['last_operation_identity'][
                    cur_phase_identity
                ] = f"{cur_phase_identity}_{operation['nop']}"

        return self.cache['last_operation_identity'].get(phase_identity)

    def get_entity_last_phase(self, entity_id):
        if 'main_routes' not in self.cache:
            entity_routes = self._get_from_rest_collection(
                'entity_route'
            )
            self.cache['main_routes'] = {
                entity_route['entity_id']: entity_route
                for entity_route in filter(
                    lambda x: x['alternate'] is False,
                    entity_routes
                )
            }

        main_routes = self.cache['main_routes']

        if entity_id not in main_routes:
            return None

        entity_route_id = main_routes[entity_id]['id']

        if 'last_operations_entity_route_id' not in self.cache:

            operations = sorted(
                self._get_from_rest_collection('operation'),
                key=lambda x: x['nop']
            )

            self.cache['last_operations_entity_route_id'] = {}
            for row in operations:
                self.cache[
                    'last_operations_entity_route_id'
                ][row['entity_route_id']] = row

        last_operations_entity_route_id = self.cache[
            'last_operations_entity_route_id'
        ]

        try:
            operation = last_operations_entity_route_id[entity_route_id]
        except IndexError:
            return None

        return self.get_phase_with_operation_id(operation['id'])

    def get_entity_first_phase(self, entity_id):
        if 'main_routes' not in self.cache:
            entity_routes = self._get_from_rest_collection(
                'entity_route'
            )
            self.cache['main_routes'] = {
                entity_route['entity_id']: entity_route
                for entity_route in filter(
                    lambda x: x['alternate'] is False,
                    entity_routes
                )
            }

        main_routes = self.cache['main_routes']

        if entity_id not in main_routes:
            return None

        entity_route_id = main_routes[entity_id]['id']

        if 'first_operations_entity_route_id' not in self.cache:

            operations = sorted(
                self._get_from_rest_collection('operation'),
                key=lambda x: x['nop'],
                reverse=True
            )

            self.cache['first_operations_entity_route_id'] = {}
            for row in operations:
                self.cache[
                    'first_operations_entity_route_id'
                ][row['entity_route_id']] = row

        first_operations_entity_route_id = self.cache[
            'first_operations_entity_route_id'
        ]

        try:
            operation = first_operations_entity_route_id[entity_route_id]
        except IndexError:
            return None

        return self.get_phase_with_operation_id(operation['id'])

    def get_entity_id(self, entity_identity):
        if 'entity_id' not in self.cache:
            self.cache['entity_id'] = {}
        else:
            return self.cache['entity_id'].get(entity_identity)
        entities_dict = list_to_dict(
            self._get_from_rest_collection('entity')
        )
        for entity_id, row in entities_dict.items():
            self.cache['entity_id'][row['identity']] = entity_id

        return self.cache['entity_id'].get(entity_identity)

    @classmethod
    def from_config(cls, config):
        return cls(
            config['login'],
            config['password'],
            config['url'],
        )

# -*- coding: utf-8 -*-
from __future__ import unicode_literals, print_function
import time
import os
import copy

import requests
import backoff

from requests.adapters import HTTPAdapter
import six

from .contract import KongAdminContract, APIAdminContract, ConsumerAdminContract, PluginAdminContract, \
    APIPluginConfigurationAdminContract, BasicAuthAdminContract, KeyAuthAdminContract, OAuth2AdminContract
from .utils import add_url_params, assert_dict_keys_in, ensure_trailing_slash
from .compat import OK, CREATED, NO_CONTENT, NOT_FOUND, CONFLICT, INTERNAL_SERVER_ERROR, urljoin, utf8_or_str
from .exceptions import ConflictError, ServerError

# WTF: As this is CI/Test specific, maybe better to only have this piece of code in your tests directory?

########################################################################################################################
# BEGIN: CI fixes
#
#   Because of memory/performance limitations in the CI, it often happened that connections to Kong got messed up
#   during unittests. To prevent this from happening, we've implemented both throttling and connection dropping as
#   optional measures during testing.
########################################################################################################################

# Minimum interval between requests (measured in seconds)
KONG_MINIMUM_REQUEST_INTERVAL = float(os.getenv('KONG_MINIMUM_REQUEST_INTERVAL', 0))

# Whether or not to reuse connections after a request (1 = true, otherwise false)
KONG_REUSE_CONNECTIONS = int(os.getenv('KONG_REUSE_CONNECTIONS', '1')) == 1


def get_default_kong_headers():
    headers = {}
    if not KONG_REUSE_CONNECTIONS:
        headers.update({'Connection': 'close'})
    return headers


class ThrottlingHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        super(ThrottlingHTTPAdapter, self).__init__(*args, **kwargs)
        self._last_request = None

    def send(self, request, stream=False, timeout=None, verify=True, cert=None, proxies=None):
        if self._last_request is not None and KONG_MINIMUM_REQUEST_INTERVAL > 0:
            diff = (self._last_request + KONG_MINIMUM_REQUEST_INTERVAL) - time.time()
            if diff > 0:
                time.sleep(diff)
        result = super(ThrottlingHTTPAdapter, self).send(request, stream, timeout, verify, cert, proxies)
        self._last_request = time.time()
        return result

# Create a singleton
THROTTLING_ADAPTER = ThrottlingHTTPAdapter()

########################################################################################################################
# END: CI fixes
########################################################################################################################


def raise_response_error(response, exception_class=None):
    exception_class = exception_class or ValueError
    assert issubclass(exception_class, BaseException)
    raise exception_class(response.content)

INVALID_FIELD_ERROR_TEMPLATE = '%r is not a valid field. Allowed fields: %r'


class RestClient(object):
    def __init__(self, api_url, headers=None):
        self.api_url = api_url
        self.headers = headers
        self._session = None

    def destroy(self):
        self.api_url = None
        self.headers = None

        if self._session is not None:
            self._session.close()
        self._session = None

    @property
    def session(self):
        if self._session is None:
            self._session = requests.session()
            if KONG_MINIMUM_REQUEST_INTERVAL > 0:
                self._session.mount(self.api_url, THROTTLING_ADAPTER)
        elif not KONG_REUSE_CONNECTIONS:
            self._session.close()
            self._session = None
            return self.session
        return self._session

    def get_headers(self, **headers):
        result = {}
        result.update(self.headers)
        result.update(headers)
        return result

    def get_url(self, *path, **query_params):
        # WTF: Never use str, unless in some very specific cases, like in compatibility layers! Fixed for you.
        path = [six.text_type(p) for p in path]
        url = ensure_trailing_slash(urljoin(self.api_url, '/'.join(path)))
        return add_url_params(url, query_params)


class APIPluginConfigurationAdminClient(APIPluginConfigurationAdminContract, RestClient):
    def __init__(self, api_admin, api_name_or_id, api_url):
        super(APIPluginConfigurationAdminClient, self).__init__(api_url, headers=get_default_kong_headers())

        self.api_admin = api_admin
        self.api_name_or_id = api_name_or_id

    def destroy(self):
        super(APIPluginConfigurationAdminClient, self).destroy()
        self.api_admin = None
        self.api_name_or_id = None

    def create(self, plugin_name, enabled=None, consumer_id=None, **fields):
        values = {}
        for key in fields:
            values['config.%s' % key] = fields[key]

        data = dict({
            'name': plugin_name,
            'consumer_id': consumer_id,
        }, **values)

        if enabled is not None and isinstance(enabled, bool):
            data['enabled'] = enabled

        response = self.session.post(self.get_url('apis', self.api_name_or_id, 'plugins'), data=data,
                                     headers=self.get_headers())

        if response.status_code == CONFLICT:
            raise_response_error(response, ConflictError)
        elif response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code != CREATED:
            raise_response_error(response, ValueError)

        return response.json()

    def create_or_update(self, plugin_name, plugin_configuration_id=None, enabled=None, consumer_id=None, **fields):
        values = {}
        for key in fields:
            values['config.%s' % key] = fields[key]

        data = dict({
            'name': plugin_name,
            'consumer_id': consumer_id,
        }, **values)

        if enabled is not None and isinstance(enabled, bool):
            data['enabled'] = enabled

        if plugin_configuration_id is not None:
            data['id'] = plugin_configuration_id

        response = self.session.put(self.get_url('apis', self.api_name_or_id, 'plugins'), data=data,
                                    headers=self.get_headers())

        if response.status_code == CONFLICT:
            raise_response_error(response, ConflictError)
        elif response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code not in (CREATED, OK):
            raise_response_error(response, ValueError)

        return response.json()

    def update(self, plugin_id, enabled=None, consumer_id=None, **fields):
        values = {}
        for key in fields:
            values['config.%s' % key] = fields[key]

        data_struct_update = copy.copy(values)

        if consumer_id is not None:
            data_struct_update['consumer_id'] = consumer_id

        if enabled is not None and isinstance(enabled, bool):
            data_struct_update['enabled'] = enabled

        url = self.get_url('apis', self.api_name_or_id, 'plugins', plugin_id)

        response = self.session.patch(url, data=data_struct_update, headers=self.get_headers())

        if response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code != OK:
            raise_response_error(response, ValueError)

        return response.json()

    @backoff.on_exception(backoff.expo, ServerError, max_tries=3)
    def list(self, size=100, offset=None, **filter_fields):
        assert_dict_keys_in(filter_fields, ['id', 'name', 'api_id', 'consumer_id'], INVALID_FIELD_ERROR_TEMPLATE)

        query_params = filter_fields
        query_params['size'] = size

        if offset is not None:
            query_params['offset'] = offset

        url = self.get_url('apis', self.api_name_or_id, 'plugins', **query_params)
        response = self.session.get(url, headers=self.get_headers())

        if response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code != OK:
            raise_response_error(response, ValueError)

        return response.json()

    @backoff.on_exception(backoff.expo, ValueError, max_tries=3)
    def delete(self, plugin_id):
        response = self.session.delete(self.get_url('apis', self.api_name_or_id, 'plugins', plugin_id),
                                       headers=self.get_headers())

        if response.status_code not in (NO_CONTENT, NOT_FOUND):
            raise ValueError('Could not delete Plugin Configuration (status: %s): %s' % (
                response.status_code, plugin_id))

    @backoff.on_exception(backoff.expo, ServerError, max_tries=3)
    def retrieve(self, plugin_id):
        response = self.session.get(self.get_url('apis', self.api_name_or_id, 'plugins', plugin_id),
                                    headers=self.get_headers())

        if response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code != OK:
            raise_response_error(response, ValueError)

        return response.json()

    @backoff.on_exception(backoff.expo, ServerError, max_tries=3)
    def count(self):
        response = self.session.get(self.get_url('apis', self.api_name_or_id, 'plugins'), headers=self.get_headers())

        if response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)

        result = response.json()
        amount = result.get('total', len(result.get('data')))
        return amount


class APIAdminClient(APIAdminContract, RestClient):
    def __init__(self, api_url):
        super(APIAdminClient, self).__init__(api_url, headers=get_default_kong_headers())

    def destroy(self):
        super(APIAdminClient, self).destroy()

    @backoff.on_exception(backoff.expo, ServerError, max_tries=3)
    def count(self):
        response = self.session.get(self.get_url('apis'), headers=self.get_headers())

        if response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)

        result = response.json()
        amount = result.get('total', len(result.get('data')))
        return amount

    def create(self, upstream_url, name=None, request_host=None, request_path=None, strip_request_path=False,
               preserve_host=False):
        response = self.session.post(self.get_url('apis'), data={
            'name': name,
            'request_host': request_host or None,  # Empty strings are not allowed
            'request_path': request_path or None,  # Empty strings are not allowed
            'strip_request_path': strip_request_path,
            'preserve_host': preserve_host,
            'upstream_url': upstream_url
        }, headers=self.get_headers())

        if response.status_code == CONFLICT:
            raise_response_error(response, ConflictError)
        elif response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code != CREATED:
            raise_response_error(response, ValueError)

        return response.json()

    def create_or_update(self, upstream_url, api_id=None, name=None, request_host=None, request_path=None,
                         strip_request_path=False, preserve_host=False):
        data = {
            'name': name,
            'request_host': request_host or None,  # Empty strings are not allowed
            'request_path': request_path or None,  # Empty strings are not allowed
            'strip_request_path': strip_request_path,
            'preserve_host': preserve_host,
            'upstream_url': upstream_url
        }

        if api_id is not None:
            data['id'] = api_id

        response = self.session.put(self.get_url('apis'), data=data, headers=self.get_headers())

        if response.status_code == CONFLICT:
            raise_response_error(response, ConflictError)
        elif response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code not in (CREATED, OK):
            raise_response_error(response, ValueError)

        return response.json()

    def update(self, name_or_id, upstream_url, **fields):
        assert_dict_keys_in(
            fields, ['name', 'request_host', 'request_path', 'strip_request_path', 'preserve_host'],
            INVALID_FIELD_ERROR_TEMPLATE)

        # Explicitly encode on beforehand before passing to requests!
        fields = dict((k, utf8_or_str(v)) if isinstance(v, six.text_type) else v for k, v in fields.items())

        response = self.session.patch(self.get_url('apis', name_or_id), data=dict({
            'upstream_url': upstream_url
        }, **fields), headers=self.get_headers())

        if response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code != OK:
            raise_response_error(response, ValueError)

        return response.json()

    @backoff.on_exception(backoff.expo, ValueError, max_tries=3)
    def delete(self, name_or_id):
        response = self.session.delete(self.get_url('apis', name_or_id), headers=self.get_headers())

        if response.status_code not in (NO_CONTENT, NOT_FOUND):
            raise ValueError('Could not delete API (status: %s): %s' % (response.status_code, name_or_id))

    @backoff.on_exception(backoff.expo, ServerError, max_tries=3)
    def retrieve(self, name_or_id):
        response = self.session.get(self.get_url('apis', name_or_id), headers=self.get_headers())

        if response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code != OK:
            raise_response_error(response, ValueError)

        return response.json()

    @backoff.on_exception(backoff.expo, ServerError, max_tries=3)
    def list(self, size=100, offset=None, **filter_fields):
        assert_dict_keys_in(filter_fields, ['id', 'name', 'request_host', 'request_path'], INVALID_FIELD_ERROR_TEMPLATE)

        query_params = filter_fields
        query_params['size'] = size

        if offset:
            query_params['offset'] = offset

        url = self.get_url('apis', **query_params)
        response = self.session.get(url, headers=self.get_headers())

        if response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code != OK:
            raise_response_error(response, ValueError)

        return response.json()

    def plugins(self, name_or_id):
        return APIPluginConfigurationAdminClient(self, name_or_id, self.api_url)


class BasicAuthAdminClient(BasicAuthAdminContract, RestClient):
    def __init__(self, consumer_admin, consumer_id, api_url):
        super(BasicAuthAdminClient, self).__init__(api_url, headers=get_default_kong_headers())

        self.consumer_admin = consumer_admin
        self.consumer_id = consumer_id

    def destroy(self):
        super(BasicAuthAdminClient, self).destroy()
        self.consumer_admin = None
        self.consumer_id = None

    def create_or_update(self, basic_auth_id=None, username=None, password=None):
        data = {
            'username': utf8_or_str(username),
            'password': utf8_or_str(password),
        }

        if basic_auth_id is not None:
            data['id'] = basic_auth_id

        response = self.session.put(self.get_url('consumers', self.consumer_id, 'basic-auth'), data=data,
                                    headers=self.get_headers())

        if response.status_code == CONFLICT:
            raise_response_error(response, ConflictError)
        elif response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code not in (CREATED, OK):
            raise_response_error(response, ValueError)

        return response.json()

    def create(self, username, password):
        response = self.session.post(self.get_url('consumers', self.consumer_id, 'basic-auth'), data={
            'username': utf8_or_str(username),
            'password': utf8_or_str(password),
        }, headers=self.get_headers())

        if response.status_code == CONFLICT:
            raise_response_error(response, ConflictError)
        elif response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code != CREATED:
            raise_response_error(response, ValueError)

        return response.json()

    @backoff.on_exception(backoff.expo, ServerError, max_tries=3)
    def list(self, size=100, offset=None, **filter_fields):
        assert_dict_keys_in(filter_fields, ['id', 'username'], INVALID_FIELD_ERROR_TEMPLATE)

        query_params = filter_fields
        query_params['size'] = size

        if offset:
            query_params['offset'] = offset

        url = self.get_url('consumers', self.consumer_id, 'basic-auth', **query_params)
        response = self.session.get(url, headers=self.get_headers())

        if response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code != OK:
            raise_response_error(response, ValueError)

        return response.json()

    @backoff.on_exception(backoff.expo, ValueError, max_tries=3)
    def delete(self, basic_auth_id):
        url = self.get_url('consumers', self.consumer_id, 'basic-auth', basic_auth_id)
        response = self.session.delete(url, headers=self.get_headers())

        if response.status_code not in (NO_CONTENT, NOT_FOUND):
            raise ValueError('Could not delete Basic Auth (status: %s): %s for Consumer: %s' % (
                response.status_code, basic_auth_id, self.consumer_id))

    @backoff.on_exception(backoff.expo, ServerError, max_tries=3)
    def retrieve(self, basic_auth_id):
        response = self.session.get(self.get_url('consumers', self.consumer_id, 'basic-auth', basic_auth_id),
                                    headers=self.get_headers())

        if response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code != OK:
            raise_response_error(response, ValueError)

        return response.json()

    @backoff.on_exception(backoff.expo, ServerError, max_tries=3)
    def count(self):
        response = self.session.get(self.get_url('consumers', self.consumer_id, 'basic-auth'),
                                    headers=self.get_headers())

        if response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code != OK:
            raise_response_error(response, ValueError)

        result = response.json()
        amount = result.get('total', len(result.get('data')))
        return amount

    def update(self, basic_auth_id, **fields):
        assert_dict_keys_in(fields, ['username', 'password'], INVALID_FIELD_ERROR_TEMPLATE)
        response = self.session.patch(
            self.get_url('consumers', self.consumer_id, 'basic-auth', basic_auth_id), data=fields,
            headers=self.get_headers())

        if response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code != OK:
            raise_response_error(response, ValueError)

        return response.json()


class KeyAuthAdminClient(KeyAuthAdminContract, RestClient):
    def __init__(self, consumer_admin, consumer_id, api_url):
        super(KeyAuthAdminClient, self).__init__(api_url, headers=get_default_kong_headers())

        self.consumer_admin = consumer_admin
        self.consumer_id = consumer_id

    def destroy(self):
        super(KeyAuthAdminClient, self).destroy()
        self.consumer_admin = None
        self.consumer_id = None

    def create_or_update(self, key_auth_id=None, key=None):
        data = {
            'key': key
        }

        if key_auth_id is not None:
            data['id'] = key_auth_id

        response = self.session.put(self.get_url('consumers', self.consumer_id, 'key-auth'), data=data,
                                    headers=self.get_headers())

        if response.status_code == CONFLICT:
            raise_response_error(response, ConflictError)
        elif response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code not in (CREATED, OK):
            raise_response_error(response, ValueError)

        return response.json()

    def create(self, key=None):
        response = self.session.post(self.get_url('consumers', self.consumer_id, 'key-auth'), data={
            'key': key,
        }, headers=self.get_headers())

        if response.status_code == CONFLICT:
            raise_response_error(response, ConflictError)
        elif response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code != CREATED:
            raise_response_error(response, ValueError)

        return response.json()

    @backoff.on_exception(backoff.expo, ServerError, max_tries=3)
    def list(self, size=100, offset=None, **filter_fields):
        assert_dict_keys_in(filter_fields, ['id', 'key'], INVALID_FIELD_ERROR_TEMPLATE)

        query_params = filter_fields
        query_params['size'] = size

        if offset:
            query_params['offset'] = offset

        url = self.get_url('consumers', self.consumer_id, 'key-auth', **query_params)
        response = self.session.get(url, headers=self.get_headers())

        if response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code != OK:
            raise_response_error(response, ValueError)

        return response.json()

    @backoff.on_exception(backoff.expo, ValueError, max_tries=3)
    def delete(self, key_auth_id):
        url = self.get_url('consumers', self.consumer_id, 'key-auth', key_auth_id)
        response = self.session.delete(url, headers=self.get_headers())

        if response.status_code not in (NO_CONTENT, NOT_FOUND):
            raise ValueError('Could not delete Key Auth (status: %s): %s for Consumer: %s' % (
                response.status_code, key_auth_id, self.consumer_id))

    @backoff.on_exception(backoff.expo, ServerError, max_tries=3)
    def retrieve(self, key_auth_id):
        response = self.session.get(self.get_url('consumers', self.consumer_id, 'key-auth', key_auth_id),
                                    headers=self.get_headers())

        if response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code != OK:
            raise_response_error(response, ValueError)

        return response.json()

    @backoff.on_exception(backoff.expo, ServerError, max_tries=3)
    def count(self):
        response = self.session.get(self.get_url('consumers', self.consumer_id, 'key-auth'),
                                    headers=self.get_headers())

        if response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code != OK:
            raise_response_error(response, ValueError)

        result = response.json()
        amount = result.get('total', len(result.get('data')))
        return amount

    def update(self, key_auth_id, **fields):
        assert_dict_keys_in(fields, ['key'], INVALID_FIELD_ERROR_TEMPLATE)
        response = self.session.patch(
            self.get_url('consumers', self.consumer_id, 'key-auth', key_auth_id), data=fields,
            headers=self.get_headers())

        if response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code != OK:
            raise_response_error(response, ValueError)

        return response.json()


class OAuth2AdminClient(OAuth2AdminContract, RestClient):
    def __init__(self, consumer_admin, consumer_id, api_url):
        super(OAuth2AdminClient, self).__init__(api_url, headers=get_default_kong_headers())

        self.consumer_admin = consumer_admin
        self.consumer_id = consumer_id

    def destroy(self):
        super(OAuth2AdminClient, self).destroy()
        self.consumer_admin = None
        self.consumer_id = None

    def create_or_update(self, oauth2_id=None, name=None, redirect_uri=None, client_id=None, client_secret=None):
        data = {
            'name': name,
            'redirect_uri': redirect_uri,
            'client_id': client_id,
            'client_secret': client_secret
        }

        if oauth2_id is not None:
            data['id'] = oauth2_id

        response = self.session.put(self.get_url('consumers', self.consumer_id, 'oauth2'), data=data,
                                    headers=self.get_headers())

        if response.status_code == CONFLICT:
            raise_response_error(response, ConflictError)
        elif response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code not in (CREATED, OK):
            raise_response_error(response, ValueError)

        return response.json()

    def create(self, name, redirect_uri, client_id=None, client_secret=None):
        response = self.session.post(self.get_url('consumers', self.consumer_id, 'oauth2'), data={
            'name': name,
            'redirect_uri': redirect_uri,
            'client_id': client_id,
            'client_secret': client_secret
        }, headers=self.get_headers())

        if response.status_code == CONFLICT:
            raise_response_error(response, ConflictError)
        elif response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code != CREATED:
            raise_response_error(response, ValueError)

        return response.json()

    @backoff.on_exception(backoff.expo, ServerError, max_tries=3)
    def list(self, size=100, offset=None, **filter_fields):
        assert_dict_keys_in(filter_fields, ['id', 'name', 'redirect_url', 'client_id'], INVALID_FIELD_ERROR_TEMPLATE)

        query_params = filter_fields
        query_params['size'] = size

        if offset:
            query_params['offset'] = offset

        url = self.get_url('consumers', self.consumer_id, 'oauth2', **query_params)
        response = self.session.get(url, headers=self.get_headers())

        if response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code != OK:
            raise_response_error(response, ValueError)

        return response.json()

    @backoff.on_exception(backoff.expo, ValueError, max_tries=3)
    def delete(self, oauth2_id):
        url = self.get_url('consumers', self.consumer_id, 'oauth2', oauth2_id)
        response = self.session.delete(url, headers=self.get_headers())

        if response.status_code not in (NO_CONTENT, NOT_FOUND):
            raise ValueError('Could not delete OAuth2 (status: %s): %s for Consumer: %s' % (
                response.status_code, oauth2_id, self.consumer_id))

    @backoff.on_exception(backoff.expo, ServerError, max_tries=3)
    def retrieve(self, oauth2_id):
        response = self.session.get(self.get_url('consumers', self.consumer_id, 'oauth2', oauth2_id),
                                    headers=self.get_headers())

        if response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code != OK:
            raise_response_error(response, ValueError)

        return response.json()

    @backoff.on_exception(backoff.expo, ServerError, max_tries=3)
    def count(self):
        response = self.session.get(self.get_url('consumers', self.consumer_id, 'oauth2'),
                                    headers=self.get_headers())

        if response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code != OK:
            raise_response_error(response, ValueError)

        result = response.json()
        amount = result.get('total', len(result.get('data')))
        return amount

    def update(self, oauth2_id, **fields):
        assert_dict_keys_in(
            fields, ['name', 'redirect_uri', 'client_id', 'client_secret'], INVALID_FIELD_ERROR_TEMPLATE)
        response = self.session.patch(
            self.get_url('consumers', self.consumer_id, 'oauth2', oauth2_id), data=fields,
            headers=self.get_headers())

        if response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code != OK:
            raise_response_error(response, ValueError)

        return response.json()


class ConsumerAdminClient(ConsumerAdminContract, RestClient):
    def __init__(self, api_url):
        super(ConsumerAdminClient, self).__init__(api_url, headers=get_default_kong_headers())

    def destroy(self):
        super(ConsumerAdminClient, self).destroy()

    @backoff.on_exception(backoff.expo, ServerError, max_tries=3)
    def count(self):
        response = self.session.get(self.get_url('consumers'), headers=self.get_headers())

        if response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)

        result = response.json()
        amount = result.get('total', len(result.get('data')))
        return amount

    def create(self, username=None, custom_id=None):
        response = self.session.post(self.get_url('consumers'), data={
            'username': username,
            'custom_id': custom_id,
        }, headers=self.get_headers())

        if response.status_code == CONFLICT:
            raise_response_error(response, ConflictError)
        elif response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code != CREATED:
            raise_response_error(response, ValueError)

        return response.json()

    def create_or_update(self, consumer_id=None, username=None, custom_id=None):
        data = {
            'username': username,
            'custom_id': custom_id,
        }

        if consumer_id is not None:
            data['id'] = consumer_id

        response = self.session.put(self.get_url('consumers'), data=data, headers=self.get_headers())

        if response.status_code == CONFLICT:
            raise_response_error(response, ConflictError)
        elif response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code not in (CREATED, OK):
            raise_response_error(response, ValueError)

        return response.json()

    def update(self, username_or_id, **fields):
        assert_dict_keys_in(fields, ['username', 'custom_id'], INVALID_FIELD_ERROR_TEMPLATE)
        response = self.session.patch(self.get_url('consumers', username_or_id), data=fields,
                                      headers=self.get_headers())

        if response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code != OK:
            raise_response_error(response, ValueError)

        return response.json()

    @backoff.on_exception(backoff.expo, ServerError, max_tries=3)
    def list(self, size=100, offset=None, **filter_fields):
        assert_dict_keys_in(filter_fields, ['id', 'custom_id', 'username'], INVALID_FIELD_ERROR_TEMPLATE)

        query_params = filter_fields
        query_params['size'] = size

        if offset:
            query_params['offset'] = offset

        url = self.get_url('consumers', **query_params)
        response = self.session.get(url, headers=self.get_headers())

        if response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code != OK:
            raise_response_error(response, ValueError)

        return response.json()

    @backoff.on_exception(backoff.expo, ValueError, max_tries=3)
    def delete(self, username_or_id):
        response = self.session.delete(self.get_url('consumers', username_or_id), headers=self.get_headers())

        if response.status_code not in (NO_CONTENT, NOT_FOUND):
            raise ValueError('Could not delete Consumer (status: %s): %s' % (response.status_code, username_or_id))

    @backoff.on_exception(backoff.expo, ServerError, max_tries=3)
    def retrieve(self, username_or_id):
        response = self.session.get(self.get_url('consumers', username_or_id), headers=self.get_headers())

        if response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code != OK:
            raise_response_error(response, ValueError)

        return response.json()

    def basic_auth(self, username_or_id):
        return BasicAuthAdminClient(self, username_or_id, self.api_url)

    def key_auth(self, username_or_id):
        return KeyAuthAdminClient(self, username_or_id, self.api_url)

    def oauth2(self, username_or_id):
        return OAuth2AdminClient(self, username_or_id, self.api_url)


class PluginAdminClient(PluginAdminContract, RestClient):
    def __init__(self, api_url):
        super(PluginAdminClient, self).__init__(api_url, headers=get_default_kong_headers())

    def destroy(self):
        super(PluginAdminClient, self).destroy()

    @backoff.on_exception(backoff.expo, ServerError, max_tries=3)
    def list(self):
        response = self.session.get(self.get_url('plugins'), headers=self.get_headers())

        if response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code != OK:
            raise_response_error(response, ValueError)

        return response.json()

    @backoff.on_exception(backoff.expo, ServerError, max_tries=3)
    def retrieve_schema(self, plugin_name):
        response = self.session.get(self.get_url('plugins', plugin_name, 'schema'), headers=self.get_headers())

        if response.status_code == INTERNAL_SERVER_ERROR:
            raise_response_error(response, ServerError)
        elif response.status_code != OK:
            raise_response_error(response, ValueError)

        return response.json()


class KongAdminClient(KongAdminContract):
    def __init__(self, api_url):
        super(KongAdminClient, self).__init__(
            apis=APIAdminClient(api_url),
            consumers=ConsumerAdminClient(api_url),
            plugins=PluginAdminClient(api_url))

    def close(self):
        self.apis.destroy()
        self.consumers.destroy()
        self.plugins.destroy()

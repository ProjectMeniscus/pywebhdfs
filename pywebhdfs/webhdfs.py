import httplib
import urllib

import requests

import pywebhdfs.errors as errors
import pywebhdfs.operations as operations


class PyWebHdfsClient(object):

    def __init__(self, host='localhost', port='50070', user_name=None):

        self.host = host
        self.port = port
        self.user_name = user_name

        self.base_uri = 'http://{host}:{port}/webhdfs/v1/'.format(
            host=self.host, port=self.port)

    def temp_create_file(self, path, file_data, **kwargs):

        optional_args = kwargs
        uri = self._create_uri(path, operations.CREATE, **optional_args)
        response = requests.put(uri, data=file_data, allow_redirects=True)

        if not response.status_code == httplib.CREATED:
            raise errors.PyWebHdfsException(response.text)

        return response.header['location']

    def create_file(self, path, file_data, **kwargs):

        optional_args = kwargs
        uri = self._create_uri(path, operations.CREATE, **optional_args)
        init_response = requests.put(uri, data=file_data,
                                     allow_redirects=False)

        if init_response.status_code is not(httplib.TEMPORARY_REDIRECT):
            raise errors.PyWebHdfsException(init_response.text)

        uri = init_response.headers['location']
        response = requests.put(uri, data=file_data)

        if response.status_code is not(httplib.CREATED):
            raise errors.PyWebHdfsException(response.text)

        return response.header['location']

    def append_file(self, path, file_data, **kwargs):

        optional_args = kwargs
        uri = self._create_uri(path, operations.APPEND, **optional_args)
        init_response = requests.post(uri, data=file_data,
                                      allow_redirects=False)

        if init_response.status_code is not(httplib.TEMPORARY_REDIRECT):
            raise errors.PyWebHdfsException(init_response.text)

        uri = init_response.headers['location']
        response = requests.post(uri, data=file_data)

        if response.status_code is not(httplib.OK):
            raise errors.PyWebHdfsException(response.text)

        return True

    def read_file(self, path, **kwargs):

        optional_args = kwargs
        uri = self._create_uri(path, operations.OPEN, **optional_args)

        response = requests.get(uri, allow_redirects=True)

        if response.status_code is not(httplib.OK):
            raise errors.PyWebHdfsException(response.text)

        return response.text

    def make_dir(self, path, **kwargs):

        optional_args = kwargs
        uri = self._create_uri(path, operations.MKDIRS, **optional_args)

        response = requests.put(uri, allow_redirects=False)

        if response.status_code is not(httplib.OK):
            raise errors.PyWebHdfsException(response.text)

        return True

    def rename_file_dir(self, path, destination_path):

        uri = self._create_uri(path, operations.RENAME,
                               destination=destination_path)

        response = requests.put(uri, allow_redirects=True)

        if response.status_code is not(httplib.OK):
            raise errors.PyWebHdfsException(response.text)

        return True

    def delete_file_dir(self, path, recursive='false'):

        uri = self._create_uri(path, operations.DELETE, recursive=recursive)
        response = requests.delete(uri, allow_redirects=True)

        if response.status_code is not(httplib.OK):
            raise errors.PyWebHdfsException(response.text)

        return True

    def get_file_dir_status(self, path):

        uri = self._create_uri(path, operations.GETFILESTATUS)
        response = requests.get(uri)

        if response.status_code is not(httplib.OK):
            raise errors.PyWebHdfsException(response.text)

        return response.json()

    def list_dir(self, path):

        uri = self._create_uri(path, operations.LISTSTATUS)
        response = requests.get(uri)

        if response.status_code is not(httplib.OK):
            raise errors.PyWebHdfsException(response.text)

        return response.json()


    def _create_uri(self, path, operation, **kwargs):

        path_param = path

        operation_param = '?op={operation}'.format(operation=operation)

        auth_param = str()

        if self.user_name:
            auth_param = '&user.name={user_name}'.format(
                user_name=self.user_name)

        keyword_params = str()
        for key in kwargs:
            keyword_params = '{params}&{key}={value}'.format(
                params=keyword_params,key=key, value=kwargs[key])

        uri = '{base_uri}{path}{operation}{keyword_args}{auth}'.format(
            base_uri=self.base_uri, path=path_param,
            operation=operation_param, keyword_args=keyword_params,
            auth=auth_param)

        return uri


if __name__ == '__main__':

    hdfs = PyWebHdfsClient(host='162.209.58.14', port='50070',
                         user_name='hduser')
    #hdfs.create_file('user/hdfs/client_test4.txt', 'mydatamydatamydata')
    #hdfs.append_file('user/hdfs/client_test4.txt', 'mydatamydatamydata')
    #print hdfs.read_file('user/hdfs/client_test4.txt', buffersize=32)
    #print hdfs.make_dir('blah')
    #print hdfs.rename_file_dir('blah', '/foo')
    #print hdfs.delete_file_dir('foo', recursive='true')
    print hdfs.get_file_dir_status('user/hdfs')
    print hdfs.list_dir('user/hdfs')

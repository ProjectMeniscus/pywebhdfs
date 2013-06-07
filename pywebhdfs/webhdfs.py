import httplib

import requests

from pywebhdfs import errors, operations


class PyWebHdfsClient(object):
    """
    PyWebHdfsClient is a Python wrapper for the Hadoop WebHDFS REST API
    """

    def __init__(self, host='localhost', port='50070', user_name=None):
        """
        Create a new client for interacting with WebHDFS

        Keyword arguments:
        host -- the ip address or hostname of the HDFS namenode
        port -- the port number for WebHDFS on the namenode
        user_name -- webHDFS user.name used for authentication
        """

        self.host = host
        self.port = port
        self.user_name = user_name

        #create base uri to be used in request operations
        self.base_uri = 'http://{host}:{port}/webhdfs/v1/'.format(
            host=self.host, port=self.port)

    def create_file(self, path, file_data, **kwargs):
        """
        Creates a new file on HDFS

        WebHDFS REST call:
        PUT http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATE
        [&overwrite=<true|false>][&blocksize=<LONG>][&replication=<SHORT>]
        [&permission=<OCTAL>][&buffersize=<INT>]

        Note: This function does not follow automatic redirects but
        instead uses a two step call to the API as required in the
        WebHDFS documentation
        """

        #make the initial CREATE call to the HDFS namenode
        optional_args = kwargs
        uri = self._create_uri(path, operations.CREATE, **optional_args)
        init_response = requests.put(uri, data=file_data,
                                     allow_redirects=False)

        if not init_response.status_code == httplib.TEMPORARY_REDIRECT:
            raise errors.PyWebHdfsException(init_response.text)

        #Get the address provided in the location header of the
        # initial response from the namenode and make the CREATE request
        #to the datanode
        uri = init_response.headers['location']
        response = requests.put(uri, data=file_data)

        if not response.status_code == httplib.CREATED:
            raise errors.PyWebHdfsException(response.text)

        return True

    def append_file(self, path, file_data, **kwargs):
        """
        Appends to an existing file on HDFS

        WebHDFS REST call:
        POST http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=APPEND
        [&buffersize=<INT>]

        Note: This function does not follow automatic redirects but
        instead uses a two step call to the API as required in the
        WebHDFS documentation
        """

        #make the initial APPEND call to the HDFS namenode
        optional_args = kwargs
        uri = self._create_uri(path, operations.APPEND, **optional_args)
        init_response = requests.post(uri, data=file_data,
                                      allow_redirects=False)

        if not init_response.status_code == httplib.TEMPORARY_REDIRECT:
            raise errors.PyWebHdfsException(init_response.text)

        #Get the address provided in the location header of the
        # initial response from the namenode and make the APPEND request
        #to the datanode
        uri = init_response.headers['location']
        response = requests.post(uri, data=file_data)

        if not response.status_code == httplib.OK:
            raise errors.PyWebHdfsException(response.text)

        return True

    def read_file(self, path, **kwargs):
        """
        Reads from a file on HDFS  and returns the content

        WebHDFS REST call:
        GET http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=OPEN
        [&offset=<LONG>][&length=<LONG>][&buffersize=<INT>]

        Note: this function follows automatic redirects
        """

        optional_args = kwargs
        uri = self._create_uri(path, operations.OPEN, **optional_args)

        response = requests.get(uri, allow_redirects=True)

        if not response.status_code == httplib.OK:
            raise errors.PyWebHdfsException(response.text)

        return response.text

    def make_dir(self, path, **kwargs):
        """
        Create a new durectory on HDFS

        WebHDFS REST call:
        PUT http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=MKDIRS
        [&permission=<OCTAL>]
        """
        optional_args = kwargs
        uri = self._create_uri(path, operations.MKDIRS, **optional_args)

        response = requests.put(uri, allow_redirects=True)

        if not response.status_code == httplib.OK:
            raise errors.PyWebHdfsException(response.text)

        return True

    def rename_file_dir(self, path, destination_path):
        """
        Rename an existing directory or file on HDFS

        WebHDFS REST call:
        PUT <HOST>:<PORT>/webhdfs/v1/<PATH>?op=RENAME&destination=<PATH>
        """

        uri = self._create_uri(path, operations.RENAME,
                               destination=destination_path)

        response = requests.put(uri, allow_redirects=True)

        if not response.status_code == httplib.OK:
            raise errors.PyWebHdfsException(response.text)

        return True

    def delete_file_dir(self, path, recursive='false'):
        """
        Delete an existing file or directory from HDFS

        WebHDFS REST call:
        DELETE <HOST>:<PORT>/webhdfs/v1/<PATH>?op=RENAME&destination=<PATH>
        """

        uri = self._create_uri(path, operations.DELETE, recursive=recursive)
        response = requests.delete(uri, allow_redirects=True)

        if not response.status_code == httplib.OK:
            raise errors.PyWebHdfsException(response.text)

        return True

    def get_file_dir_status(self, path):
        """
        Get the file_status of a single file or directory on HDFS

        WebHDFS REST call:
        GET http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETFILESTATUS
        """

        uri = self._create_uri(path, operations.GETFILESTATUS)
        response = requests.get(uri, allow_redirects=True)

        if not response.status_code == httplib.OK:
            raise errors.PyWebHdfsException(response.text)

        return response.json()

    def list_dir(self, path):
        """
        Get a list of file_status for all files and directories
        inside an HDFS directory

        WebHDFS REST call:
        GET http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=LISTSTATUS
        """

        uri = self._create_uri(path, operations.LISTSTATUS)
        response = requests.get(uri, allow_redirects=True)

        if not response.status_code == httplib.OK:
            raise errors.PyWebHdfsException(response.text)

        return response.json()

    def _create_uri(self, path, operation, **kwargs):
        """
        internal function used to construct the WebHDFS request uri based on
        the <PATH>, <OPERATION>, and any provided optional arguments
        """

        path_param = path

        #setup the parameter represent the WebHDFS operation
        operation_param = '?op={operation}'.format(operation=operation)

        #configure authorization based on provided credentials
        auth_param = str()
        if self.user_name:
            auth_param = '&user.name={user_name}'.format(
                user_name=self.user_name)

        #setup any optiona parameters
        keyword_params = str()
        for key in kwargs:
            keyword_params = '{params}&{key}={value}'.format(
                params=keyword_params, key=key, value=kwargs[key])

        #build the complete uri from the base uri and all configured params
        uri = '{base_uri}{path}{operation}{keyword_args}{auth}'.format(
            base_uri=self.base_uri, path=path_param,
            operation=operation_param, keyword_args=keyword_params,
            auth=auth_param)

        return uri

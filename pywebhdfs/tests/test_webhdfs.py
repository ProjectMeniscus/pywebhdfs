import httplib
import unittest

from mock import MagicMock
from mock import patch

from pywebhdfs import errors
from pywebhdfs.webhdfs import PyWebHdfsClient, _raise_pywebhdfs_exception
from pywebhdfs import operations


class WhenTestingPyWebHdfsConstructor(unittest.TestCase):

    def test_init_default_args(self):
        webhdfs = PyWebHdfsClient()
        self.assertEqual('localhost', webhdfs.host)
        self.assertEqual('50070', webhdfs.port)
        self.assertIsNone(webhdfs.user_name)

    def test_init_args_provided(self):
        host = '127.0.0.1'
        port = '50075'
        user_name = 'myUser'

        webhdfs = PyWebHdfsClient(host=host, port=port, user_name=user_name)
        self.assertEqual(host, webhdfs.host)
        self.assertEqual(port, webhdfs.port)
        self.assertEqual(user_name, webhdfs.user_name)


class WhenTestingCreateOperation(unittest.TestCase):

    def setUp(self):

        self.host = 'hostname'
        self.port = '00000'
        self.user_name = 'username'
        self.webhdfs = PyWebHdfsClient(host=self.host, port=self.port,
                                       user_name=self.user_name)
        self.response = MagicMock()
        self.requests = MagicMock(return_value=self.response)
        self.location = 'redirect_uri'
        self.path = 'user/hdfs'
        self.file_data = '010101'
        self.init_response = MagicMock()
        self.init_response.headers = {'location': self.location}
        self.response = MagicMock()
        self.expected_headers = {'content-type': 'application/octet-stream'}

    def test_create_throws_exception_for_no_redirect(self):

        self.init_response.status_code = httplib.BAD_REQUEST
        self.response.status_code = httplib.CREATED
        self.requests.put.side_effect = [self.init_response, self.response]
        with patch('pywebhdfs.webhdfs.requests', self.requests):
            with self.assertRaises(errors.PyWebHdfsException):
                self.webhdfs.create_file(self.path, self.file_data)

    def test_create_throws_exception_for_not_created(self):

        self.init_response.status_code = httplib.TEMPORARY_REDIRECT
        self.response.status_code = httplib.BAD_REQUEST
        self.requests.put.side_effect = [self.init_response, self.response]
        with patch('pywebhdfs.webhdfs.requests', self.requests):
            with self.assertRaises(errors.PyWebHdfsException):
                self.webhdfs.create_file(self.path, self.file_data)

    def test_create_returns_file_location(self):

        self.init_response.status_code = httplib.TEMPORARY_REDIRECT
        self.response.status_code = httplib.CREATED
        self.put_method = MagicMock(
            side_effect=[self.init_response, self.response])
        self.requests.put = self.put_method
        with patch('pywebhdfs.webhdfs.requests', self.requests):
            result = self.webhdfs.create_file(self.path, self.file_data)
        self.assertTrue(result)
        self.put_method.assert_called_with(
            self.location, headers=self.expected_headers, data=self.file_data)


class WhenTestingAppendOperation(unittest.TestCase):

    def setUp(self):

        self.host = 'hostname'
        self.port = '00000'
        self.user_name = 'username'
        self.webhdfs = PyWebHdfsClient(host=self.host, port=self.port,
                                       user_name=self.user_name)
        self.response = MagicMock()
        self.requests = MagicMock(return_value=self.response)
        self.location = 'redirect_uri'
        self.path = 'user/hdfs'
        self.file_data = '010101'
        self.init_response = MagicMock()
        self.init_response.header = {'location': self.location}
        self.response = MagicMock()

    def test_append_throws_exception_for_no_redirect(self):

        self.init_response.status_code = httplib.BAD_REQUEST
        self.response.status_code = httplib.OK
        self.requests.post.side_effect = [self.init_response, self.response]
        with patch('pywebhdfs.webhdfs.requests', self.requests):
            with self.assertRaises(errors.PyWebHdfsException):
                self.webhdfs.append_file(self.path, self.file_data)

    def test_append_throws_exception_for_not_ok(self):

        self.init_response.status_code = httplib.TEMPORARY_REDIRECT
        self.response.status_code = httplib.BAD_REQUEST
        self.requests.post.side_effect = [self.init_response, self.response]
        with patch('pywebhdfs.webhdfs.requests', self.requests):
            with self.assertRaises(errors.PyWebHdfsException):
                self.webhdfs.append_file(self.path, self.file_data)

    def test_append_returns_true(self):

        self.init_response.status_code = httplib.TEMPORARY_REDIRECT
        self.response.status_code = httplib.OK
        self.requests.post.side_effect = [self.init_response, self.response]
        with patch('pywebhdfs.webhdfs.requests', self.requests):
            result = self.webhdfs.append_file(self.path, self.file_data)
        self.assertTrue(result)


class WhenTestingOpenOperation(unittest.TestCase):

    def setUp(self):

        self.host = 'hostname'
        self.port = '00000'
        self.user_name = 'username'
        self.webhdfs = PyWebHdfsClient(host=self.host, port=self.port,
                                       user_name=self.user_name)
        self.response = MagicMock()
        self.requests = MagicMock(return_value=self.response)
        self.path = 'user/hdfs'
        self.file_data = u'010101'
        self.response = MagicMock()
        self.response.content = self.file_data

    def test_read_throws_exception_for_not_ok(self):

        self.response.status_code = httplib.BAD_REQUEST
        self.requests.get.return_value = self.response
        with patch('pywebhdfs.webhdfs.requests', self.requests):
            with self.assertRaises(errors.PyWebHdfsException):
                self.webhdfs.read_file(self.path)

    def test_read_returns_file(self):

        self.response.status_code = httplib.OK
        self.requests.get.return_value = self.response
        with patch('pywebhdfs.webhdfs.requests', self.requests):
            result = self.webhdfs.read_file(self.path)
        self.assertEqual(result, self.file_data)


class WhenTestingMkdirOperation(unittest.TestCase):

    def setUp(self):

        self.host = 'hostname'
        self.port = '00000'
        self.user_name = 'username'
        self.webhdfs = PyWebHdfsClient(host=self.host, port=self.port,
                                       user_name=self.user_name)
        self.response = MagicMock()
        self.requests = MagicMock(return_value=self.response)
        self.path = 'user/hdfs'
        self.response = MagicMock()

    def test_mkdir_throws_exception_for_not_ok(self):

        self.response.status_code = httplib.BAD_REQUEST
        self.requests.put.return_value = self.response
        with patch('pywebhdfs.webhdfs.requests', self.requests):
            with self.assertRaises(errors.PyWebHdfsException):
                self.webhdfs.make_dir(self.path)

    def test_mkdir_returns_true(self):

        self.response.status_code = httplib.OK
        self.requests.put.return_value = self.response
        with patch('pywebhdfs.webhdfs.requests', self.requests):
            result = self.webhdfs.make_dir(self.path)
        self.assertTrue(result)


class WhenTestingRenameOperation(unittest.TestCase):

    def setUp(self):

        self.host = 'hostname'
        self.port = '00000'
        self.user_name = 'username'
        self.webhdfs = PyWebHdfsClient(host=self.host, port=self.port,
                                       user_name=self.user_name)
        self.response = MagicMock()
        self.requests = MagicMock(return_value=self.response)
        self.path = 'user/hdfs/old_dir'
        self.new_path = '/user/hdfs/new_dir'
        self.response = MagicMock()

    def test_rename_throws_exception_for_not_ok(self):

        self.response.status_code = httplib.BAD_REQUEST
        self.requests.put.return_value = self.response
        with patch('pywebhdfs.webhdfs.requests', self.requests):
            with self.assertRaises(errors.PyWebHdfsException):
                self.webhdfs.rename_file_dir(self.path, self.new_path)

    def test_rename_returns_true(self):

        self.response.status_code = httplib.OK
        self.requests.put.return_value = self.response
        with patch('pywebhdfs.webhdfs.requests', self.requests):
            result = self.webhdfs.rename_file_dir(self.path, self.new_path)
        self.assertTrue(result)


class WhenTestingDeleteOperation(unittest.TestCase):

    def setUp(self):

        self.host = 'hostname'
        self.port = '00000'
        self.user_name = 'username'
        self.webhdfs = PyWebHdfsClient(host=self.host, port=self.port,
                                       user_name=self.user_name)
        self.response = MagicMock()
        self.requests = MagicMock(return_value=self.response)
        self.path = 'user/hdfs/old_dir'
        self.response = MagicMock()

    def test_rename_throws_exception_for_not_ok(self):

        self.response.status_code = httplib.BAD_REQUEST
        self.requests.delete.return_value = self.response
        with patch('pywebhdfs.webhdfs.requests', self.requests):
            with self.assertRaises(errors.PyWebHdfsException):
                self.webhdfs.delete_file_dir(self.path)

    def test_rename_returns_true(self):

        self.response.status_code = httplib.OK
        self.requests.delete.return_value = self.response
        with patch('pywebhdfs.webhdfs.requests', self.requests):
            result = self.webhdfs.delete_file_dir(self.path)
        self.assertTrue(result)


class WhenTestingGetFileStatusOperation(unittest.TestCase):

    def setUp(self):

        self.host = 'hostname'
        self.port = '00000'
        self.user_name = 'username'
        self.webhdfs = PyWebHdfsClient(host=self.host, port=self.port,
                                       user_name=self.user_name)
        self.response = MagicMock()
        self.requests = MagicMock(return_value=self.response)
        self.path = 'user/hdfs/old_dir'
        self.response = MagicMock()
        self.file_status = {
            "FileStatus": {
                "accessTime": 0,
                "blockSize": 0,
                "group": "supergroup",
                "length": 0,
                "modificationTime": 1320173277227,
                "owner": "webuser",
                "pathSuffix": "",
                "permission": "777",
                "replication": 0,
                "type": "DIRECTORY"
            }
        }
        self.response.json = MagicMock(return_value=self.file_status)

    def test_get_status_throws_exception_for_not_ok(self):

        self.response.status_code = httplib.BAD_REQUEST
        self.requests.get.return_value = self.response
        with patch('pywebhdfs.webhdfs.requests', self.requests):
            with self.assertRaises(errors.PyWebHdfsException):
                self.webhdfs.get_file_dir_status(self.path)

    def test_get_status_returns_true(self):

        self.response.status_code = httplib.OK
        self.requests.get.return_value = self.response
        with patch('pywebhdfs.webhdfs.requests', self.requests):
            result = self.webhdfs.get_file_dir_status(self.path)

        for key in result:
            self.assertEqual(result[key], self.file_status[key])


class WhenTestingListDirOperation(unittest.TestCase):

    def setUp(self):

        self.host = 'hostname'
        self.port = '00000'
        self.user_name = 'username'
        self.webhdfs = PyWebHdfsClient(host=self.host, port=self.port,
                                       user_name=self.user_name)
        self.response = MagicMock()
        self.requests = MagicMock(return_value=self.response)
        self.path = 'user/hdfs/old_dir'
        self.response = MagicMock()
        self.file_status = {
            "FileStatuses": {
                "FileStatus": [
                    {
                        "accessTime": 0,
                        "blockSize": 0,
                        "group": "supergroup",
                        "length": 24930,
                        "modificationTime": 1320173277227,
                        "owner": "webuser",
                        "pathSuffix": "a.patch",
                        "permission": "777",
                        "replication": 0,
                        "type": "FILE"
                    },
                    {
                        "accessTime": 0,
                        "blockSize": 0,
                        "group": "supergroup",
                        "length": 0,
                        "modificationTime": 1320173277227,
                        "owner": "webuser",
                        "pathSuffix": "",
                        "permission": "777",
                        "replication": 0,
                        "type": "DIRECTORY"
                    }
                ]
            }
        }
        self.response.json = MagicMock(return_value=self.file_status)

    def test_get_status_throws_exception_for_not_ok(self):

        self.response.status_code = httplib.BAD_REQUEST
        self.requests.get.return_value = self.response
        with patch('pywebhdfs.webhdfs.requests', self.requests):
            with self.assertRaises(errors.PyWebHdfsException):
                self.webhdfs.list_dir(self.path)

    def test_get_status_returns_true(self):

        self.response.status_code = httplib.OK
        self.requests.get.return_value = self.response
        with patch('pywebhdfs.webhdfs.requests', self.requests):
            result = self.webhdfs.list_dir(self.path)

        for key in result:
            self.assertEqual(result[key], self.file_status[key])


class WhenTestingCreateUri(unittest.TestCase):

    def setUp(self):
        self.host = 'hostname'
        self.port = '00000'
        self.user_name = 'username'
        self.path = 'user/hdfs'
        self.webhdfs = PyWebHdfsClient(host=self.host, port=self.port,
                                       user_name=self.user_name)

    def test_create_uri_no_kwargs(self):
        op = operations.CREATE
        uri = 'http://{host}:{port}/webhdfs/v1/' \
              '{path}?op={op}&user.name={user}'\
            .format(
                host=self.host, port=self.port, path=self.path,
                op=op, user=self.user_name)
        result = self.webhdfs._create_uri(self.path, op)
        self.assertEqual(uri, result)

    def test_create_uri_with_kwargs(self):
        op = operations.CREATE
        mykey = 'mykey'
        myval = 'myval'
        uri = 'http://{host}:{port}/webhdfs/v1/' \
              '{path}?op={op}&{key}={val}' \
              '&user.name={user}' \
            .format(
                host=self.host, port=self.port, path=self.path,
                op=op, key=mykey, val=myval, user=self.user_name)
        result = self.webhdfs._create_uri(self.path, op, mykey=myval)
        self.assertEqual(uri, result)

    def test_create_uri_with_unicode_path(self):
        op = operations.CREATE
        mykey = 'mykey'
        myval = 'myval'
        path = u'die/Stra\xdfe'
        quoted_path = 'die/Stra%C3%9Fe'
        uri = 'http://{host}:{port}/webhdfs/v1/' \
              '{path}?op={op}&{key}={val}' \
              '&user.name={user}' \
            .format(
                host=self.host, port=self.port, path=quoted_path,
                op=op, key=mykey, val=myval, user=self.user_name)
        result = self.webhdfs._create_uri(path, op, mykey=myval)
        self.assertEqual(uri, result)


class WhenTestingRaiseExceptions(unittest.TestCase):

    def test_400_raises_bad_request(self):
        with self.assertRaises(errors.BadRequest):
            _raise_pywebhdfs_exception(httplib.BAD_REQUEST)

    def test_401_raises_unuathorized(self):
        with self.assertRaises(errors.Unauthorized):
            _raise_pywebhdfs_exception(httplib.UNAUTHORIZED)

    def test_404_raises_not_found(self):
        with self.assertRaises(errors.FileNotFound):
            _raise_pywebhdfs_exception(httplib.NOT_FOUND)

    def test_all_other_raises_pywebhdfs_exception(self):
        with self.assertRaises(errors.PyWebHdfsException):
            _raise_pywebhdfs_exception(httplib.GATEWAY_TIMEOUT)

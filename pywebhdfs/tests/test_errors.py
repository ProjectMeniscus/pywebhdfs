import unittest

from pywebhdfs.webhdfs import errors


class WhenTestingErrors(unittest.TestCase):
    def setUp(self):
        pass

    def test_pywebhdfs_exception_is_exception(self):
        self.assertIsInstance(errors.PyWebHdfsException(), Exception)
        with self.assertRaises(Exception):
            raise errors.PyWebHdfsException

    def test_pywebhdfs_exception(self):
        msg = 'message'
        ex = errors.PyWebHdfsException(msg=msg)
        self.assertIs(msg, ex.message)

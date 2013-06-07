import httplib
import unittest

from mock import MagicMock
from mock import patch

from pywebhdfs.webhdfs import PyWebHdfsClient

def suite():
    suite = unittest.TestSuite()
    suite.addTest(WhenTestingCreateOperation())
    return suite


class WhenTestingCreateOperation(unittest.TestCase):
    def setUp(self):
        pass

    def test_create_throws_exception_for_no_redirect(self):
        self.assertTrue(True)


class PyWebHdfsException(Exception):
    def __init__(self, msg=str()):
        self.msg = msg
        super(PyWebHdfsException, self).__init__(self.msg)


class BadRequest(PyWebHdfsException):
    pass


class Unauthorized(PyWebHdfsException):
    pass


class FileNotFound(PyWebHdfsException):
    pass


class MethodNotAllowed(PyWebHdfsException):
    pass

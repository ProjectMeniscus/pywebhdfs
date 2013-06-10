# PyWebHDFS

PyWebHDFS is a Python wrapper for the Hadoop WebHDFS REST API.

Many of the current Python HDFS clients rely on Hadoop Streaming which requires Java to be installed on the local machine. The other option for interacting with HDFS is to use the WebHDFS REST API.  The purpose of this project is to simplify interactions with the WebHDFS API.  The PyWebHdfs client will implement the exact functions available in the WebHDFS REST API and behave in a manner consistent with the API.

The initial release provides for basic WebHDFS file and directory operations including:

1.  Create and Write to a File
2.  Append to a File
3.  Open and Read a File
4.  Make a Directory
5.  Rename a File/Directory
6.  Delete a File/Directory
7.  Status of a File/Directory
8.  List a Directory

The documentation for the Hadoop WebHDFS REST API can be found at [http://hadoop.apache.org/docs/r1.0.4/webhdfs.html](http://hadoop.apache.org/docs/r1.0.4/webhdfs.html)

Pypi package: [https://pypi.python.org/pypi/pywebhdfs](https://pypi.python.org/pypi/pywebhdfs)


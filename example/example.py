from pywebhdfs.webhdfs import PyWebHdfsClient
import logging

logging.basicConfig(level=logging.DEBUG)
_LOG = logging.getLogger(__name__)

example_dir = 'user/hdfs/example_dir'
example_file = '{dir}/example.txt'.format(dir=example_dir)
example_data = '01010101010101010101010101010101010101010101\n'
rename_dir = 'user/hdfs/example_rename'


#create a new client instance
hdfs = PyWebHdfsClient(host='localhost', port='50070',
                       user_name='hduser')

#create a new directory for the example
print('making new HDFS directory at: {0}\n'.format(example_dir))
hdfs.make_dir(example_dir)

# get a dictionary of the directory's status
dir_status = hdfs.get_file_dir_status(example_dir)
print dir_status

# create a new file on hdfs
print('making new file at: {0}\n'.format(example_file))
hdfs.create_file(example_file, example_data)

file_status = hdfs.get_file_dir_status(example_file)
print file_status

#append to the file created in previous step
print('appending to file at: {0}\n'.format(example_file))
hdfs.append_file(example_file, example_data)

file_status = hdfs.get_file_dir_status(example_file)
print file_status

#read in the data for the file
print('rading data from file at: {0}\n'.format(example_file))
file_data = hdfs.read_file(example_file)
print file_data

#rename the example_dir
print('renaming directory from {0} to {1}\n').format(example_dir, rename_dir)
hdfs.rename_file_dir(example_dir, '/{0}'.format(rename_dir))

#list the contents of the new directory
listdir_stats = hdfs.list_dir(rename_dir)
print listdir_stats

example_file = '{dir}/example.txt'.format(dir=rename_dir)

#delete the example file
print('deleting example file at: {0}'.format(example_file))
hdfs.delete_file_dir(example_file)

#list the contents of the directory
listdir_stats = hdfs.list_dir(rename_dir)
print listdir_stats

#delete the example directory
print('deleting the example directory at: {0}'.format(rename_dir))
hdfs.delete_file_dir(rename_dir, recursive='true')

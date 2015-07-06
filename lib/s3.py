#!/usr/bin/env python

import os
import sys
import ConfigParser
from boto.s3.connection import S3Connection
from boto.s3.connection import OrdinaryCallingFormat
from boto.s3.key import Key

CONFIG_FILE = os.path.join(os.path.expanduser('~'), '.synapseS3config')


class bucketManager:
    """
    utility functions to manage s3 bucket
    """
    
    def __init__(self, aws_bucketname, aws_key = None, aws_secret=None, use_ssl = True, 
                 config_path = CONFIG_FILE, rememberMe = False):
        self.bucketname = aws_bucketname
        self.aws_url_basepath = "https://s3.amazonaws.com"
        self.configPath = config_path
        self.rememberMe = rememberMe
        
        #establish the AWS connection
        self.aws_connection = self._connect(aws_key, aws_secret, use_ssl = use_ssl)

        #establish a connection to bucket
        if not self._bucketExists(self.bucketname):
            self._printBucketNotFoundMessage(aws_bucketname)
            self.bucket = None
        else:
            self.bucket = self.aws_connection.get_bucket(aws_bucketname)


    def getFileNames(self):
        return map(lambda aws_file_key: aws_file_key.name, self.bucket.list())

    def downloadFile(self, filename, local_download_directory):
        for s3_file in self.bucket.list():
            if filename == s3_file.name:
                self._downloadFile(s3_file, local_download_directory)
                break;

    def downloadAllFiles(self, local_download_directory):
        for s3_file in self.bucket.list():
            self._downloadFile(s3_file, local_download_directory)

    def deleteAllFiles(self):
        for s3_file in self.bucket.list():
            self._deleteFile(bucket, s3_file)

    def downloadFilesWithPredicate(self, filename_predicate, local_download_destination):
        for s3_file in filter(lambda fkey: filename_predicate(fkey.name), self.bucket.list()):
            self._downloadFile(s3_file, local_download_destination)

    def deleteFilesWithPredicate(self, filename_predicate):
        for s3_file in filter(lambda fkey: filename_predicate(fkey.name), self.bucket.list()):
            self._deleteFile(bucket, s3_file)
                
    def _uploadFile(self, file_path, prefix=None):  
        k = Key(self.bucket)
        if prefix is None:
            key = os.path.basename(file_path) #generate the uniq key given a file name
        else:
            key = '%s/%s' % (prefix, os.path.basename(file_path))
        k.key = key
        k.set_contents_from_filename(file_path)
        
        file_url = '%s/%s/%s' % (self.aws_url_basepath, self.bucketname,key)
        return(file_url)
    
    def uploadFiles(self, file_list, prefix=None):
        output = {}
        if not isinstance(file_list, (list, tuple)):
            file_list = [file_list]
        for f in file_list:
            url = self._uploadFile(f,prefix)
            output[f] = url
            print 'Uploaded: %s \nURL: %s \n--- \n' % (f, url)
    
    def _bucketExists(self, bucket_name):
        return self.aws_connection.lookup(bucket_name) != None

    def _printBucketNotFoundMessage(self, bucket_name):
        print "Error: bucket '%s' not found" % bucket_name

    def _downloadFile(self, s3_file, local_download_destination):
        full_local_path = os.path.expanduser(os.path.join(local_download_destination, s3_file.name))
        try:
            print "Downloaded: %s" % (full_local_path)
            s3_file.get_contents_to_filename(full_local_path)
        except:
            print "Error downloading"

    def _deleteFile(self, bucket, s3_file):
        bucket.delete_key(s3_file)
        print "Deleted: %s" % s3_file.name

    def _connect(self, aws_key, aws_secret, use_ssl = True):
        if aws_key is None or aws_secret is None:
            sys.stderr.write('reading the AWS connection information from config file %s ' % CONFIG_FILE)
            config = self._getConfigFile(self.configPath)
            if config.has_option('AWS','aws_key'):
                aws_key = config.get('AWS', 'aws_key')
            if config.has_option('AWS','aws_secret'):
                aws_secret = config.get('AWS', 'aws_secret')
        #establish a connection
        aws_connection = S3Connection(aws_key, aws_secret, is_secure = use_ssl, calling_format=OrdinaryCallingFormat())
        sys.stderr.write('..aws connection established \n')

        if self.rememberMe == True:
            with open(CONFIG_FILE, 'w') as f:
                f.write('[AWS]\n')
                f.write('aws_key: %s\n' % aws_key)
                f.write('aws_secret: %s\n' % aws_secret) 
        return aws_connection           

    def _getConfigFile(self, configPath):
        """Returns a ConfigParser populated with properties from the user's configuration file."""
        try:
            config = ConfigParser.ConfigParser()
            config.read(configPath) # Does not fail if the file does not exist
            return config
        except ConfigParser.Error:
            sys.stderr.write('Error parsing s3upload config file: %s' % configPath)
            raise


def build_parser():
    """Builds the argument parser and returns the result."""

    parser = argparse.ArgumentParser(description='Synapse Python Utilities')
    parser.add_argument('--debug', dest='debug',  action='store_true')
    subparsers = parser.add_subparsers(title='commands',
            description='The following commands are available:',
            help='For additional help: "synutils.py <COMMAND> -h"')
    parser_s3 = subparsers.add_parser('s3',help='utilities to manage data on static.synapse.org')
    parser_s3.add_argument('-k' , '--aws_key',  dest='aws_key', help='AWS Key')
    parser_s3.add_argument('-s' , '--aws_secret', dest='aws_secret', help='AWS secret key')
    parser_s3.add_argument('-up', '--upload', dest='upload_path', type=str, default=None)
    parser_s3.add_argument('--rememberMe', '--remember-me', dest='rememberMe', action='store_true', default=False,
                           help='Cache credentials for automatic authentication for future interactions')
#    parser_s3.add_argument('-r', '--recursive', action='store_true', default=False,
#                                 help='recursively upload the directory')
    parser_s3.set_defaults(func=s3)
    return parser






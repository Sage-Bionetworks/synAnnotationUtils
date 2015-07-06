#!/usr/bin/env python

import argparse
import os
import sys

script_path = os.path.dirname(__file__)
local_module_path = os.path.abspath(os.path.join(script_path,'lib'))
sys.path.append(local_module_path)
import s3

STATIC_BUCKET = "static.synapse.org"


def s3manage(args):
	"""
	Utilities for managing S3 bukcets
	"""
	bucket = s3.bucketManager(STATIC_BUCKET, args.aws_key, args.aws_secret, rememberMe=args.rememberMe)
	bucket.uploadFiles(args.upload_path,args.upload_prefix)


def build_parser():
    """Builds the argument parser and returns the result."""

    parser = argparse.ArgumentParser(description='Synapse Python Utilities')
    parser.add_argument('--debug', dest='debug',  action='store_true')

    subparsers = parser.add_subparsers(title='commands',
            description='The following commands are available:',
            help='For additional help: "synutils.py <COMMAND> -h"')

    parser_s3 = subparsers.add_parser('s3',help='utilities to manage data on static.synapse.org')
    parser_s3.add_argument('-k' , '--aws_key',  dest='aws_key', help='AWS Key', default=None)
    parser_s3.add_argument('-s' , '--aws_secret', dest='aws_secret', help='AWS secret key', default=None)
    parser_s3.add_argument('-up', '--upload', dest='upload_path', type=str, default=None)
    parser_s3.add_argument('-p',  '--prefix', dest='upload_prefix', type=str, default='scratch/',
                           help = 'prefix adds the sub dir structure on S3 eg. test/ will add the file under test/ folder on s3 bucket')
    parser_s3.add_argument('--rememberMe', '--remember-me', dest='rememberMe', action='store_true', default=False,
            			   help='Cache credentials for automatic authentication for future interactions')
#    parser_s3.add_argument('-r', '--recursive', action='store_true', default=False,
#					              help='recursively upload the directory')
    parser_s3.set_defaults(func=s3manage)
    return parser


def perform_main(args):
    if 'func' in args:
        try:
            args.func(args)
        except Exception as ex:
            raise

def main():
	args = build_parser().parse_args()
	perform_main(args)


if __name__ == "__main__":
	main()



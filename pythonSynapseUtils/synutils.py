#!/usr/bin/env python

import argparse
import os
import sys
import synapseclient
import hashlib
import string

script_path = os.path.dirname(__file__)
local_module_path = os.path.abspath(os.path.join(script_path,'lib'))
sys.path.append(local_module_path)
import s3

STATIC_BUCKET = "static.synapse.org"


def create_html_file(html_link):
    #get a unique file name from txt/link
    html_file_name = str(hashlib.md5(html_link).hexdigest()) + '.html'
    f = open(html_file_name, 'w')
    html_template = string.Template("""
    <!DOCTYPE html>
    <html>
    <body>
    <iframe src="$HTML_LINK" width="1500" height="1000" allowfullscreen="true" mozallowfullscreen="true" webkitallowfullscreen="true"></iframe> 
    </body>
    </html>
    """)
    html_content = html_template.substitute(HTML_LINK=html_link)
    f.write(html_content)
    f.close()
    os.chmod(html_file_name, 0755) #make the file web readable before upload
    return(html_file_name)

def s3manage(args):
    """
    Utilities for managing S3 bukcets
    """
    #establish a connection to S3
    bucket = s3.bucketManager(STATIC_BUCKET, args.aws_key, args.aws_secret, rememberMe=args.rememberMe)

    #if user specifies an html link
    if args.html_link is not None:
        html_file = create_html_file(args.html_link)
        args.upload_path = html_file

    if os.path.isdir(args.upload_path) is True:
        url = bucket.uploadDir(args.upload_path,args.upload_prefix)
    else:   
        url = bucket.uploadFiles(args.upload_path,args.upload_prefix)

    if args.synapse_wikiID is not None:
        embed_url_in_synapse_wiki(url,args.synapse_wikiID)


def embed_url_in_synapse_wiki(url, wikiID):
    import synapseclient
    syn = synapseclient.login()
    wiki = syn.getWiki(wikiID)
    markdown = wiki['markdown']

    #complete hack
    if len(url) > 1:
        url = [url[x] for x in url if x.endswith('index.html')]
        url = url[0]
    else:
        url = url.values()[0]

    #percent encoded URL
    import urllib
    url = urllib.quote(url, safe='')
    link_markdown = '${iframe?site=' + url + '&height=1000}'
    wiki['markdown'] = link_markdown
    wiki = syn.store(wiki)
    syn.onweb(wikiID)


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
    parser_s3.add_argument('-l', '--link', dest='html_link', type=str, default=None,
                           help = "html link to embed in a synapse wiki")
    parser_s3.add_argument('-w', '--wikiID', dest='synapse_wikiID', type=str, default=None,
                           help = "synapse wiki id to embed the link in")
    parser_s3.add_argument('-p',  '--prefix', dest='upload_prefix', type=str, default='scratch/',
                           help = 'prefix adds the sub dir structure on S3 eg. test/ will add the file under test/ folder on s3 bucket')
    parser_s3.add_argument('--rememberMe', '--remember-me', dest='rememberMe', action='store_true', default=False,
            			   help='Cache credentials for automatic authentication for future interactions')
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



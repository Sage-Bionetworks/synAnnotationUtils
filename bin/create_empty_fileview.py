#!/usr/bin/env python

"""
Create empty file view from a Synapse annotations json file.

"""

import os
import sys
import json
import urlparse
import urllib

import synapseclient


def path2url(path):
    """Convert path to URL, even if it already is a URL.
    """

    if path.startswith("/"):
        new_path = urlparse.urljoin('file:',
                                    urllib.pathname2url(os.path.abspath(path)))
    else:
        new_path = path

    return new_path


def getSchemaFromJson(json_file, defaultMaximumSize=250):
    f = urllib.urlopen(path2url(json_file))
    data = json.load(f)

    cols = []

    for d in data:
        d['enumValues'] = [a['value'] for a in d['enumValues']]

        if d['columnType'] == 'STRING' and not d['maximumSize']:
            d['maximumSize'] = defaultMaximumSize

        cols.append(synapseclient.Column(**d))

    return cols


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Create Empty File View")
    parser.add_argument('--id', help='Synapse ID of project in which to create\
    file view')
    parser.add_argument('--name', help='Name of file view')
    parser.add_argument('-s', '--scopes',
                        help='A comma-delimited list of Synapse IDs of scopes that file view should include.')
    parser.add_argument('json', nargs='+',
                        help='One or more json files to use to define the file view Schema.')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Verbose output.')

    args = parser.parse_args()

    syn = synapseclient.login(silent=True)

    if args.verbose:
        sys.stderr.write('Preparing to create fileview\n')

    project_id = args.id
    scopes = args.scopes
    jsons = args.json
    view_name = args.name

    # get schema from json
    cols = []
    [cols.extend(getSchemaFromJson(j)) for j in jsons]

    scopes = scopes.split(',')
    fv = synapseclient.EntityViewSchema(name=view_name, parent=project_id,
                                        scopes=scopes, columns=cols)

    syn.store(fv)


if __name__ == '__main__':
    main()

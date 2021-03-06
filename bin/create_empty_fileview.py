#!/usr/bin/env python

"""Create empty file view from a Synapse annotations json file.

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


def createColumnsFromJson(json_file, defaultMaximumSize=250):
    """Create a list of Synapse Table Columns from a Synapse annotations JSON file.

    This creates a list of columns; if the column is a 'STRING' and
    defaultMaximumSize is specified, change the default maximum size for that
    column.

    """

    f = urllib.urlopen(path2url(json_file))
    data = json.load(f)

    cols = []

    for d in data:
        d['enumValues'] = [a['value'] for a in d['enumValues']]

        if d['columnType'] == 'STRING' and defaultMaximumSize:
            d['maximumSize'] = defaultMaximumSize

        cols.append(synapseclient.Column(**d))

    return cols


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Create Empty File View")
    parser.add_argument('--id', help='Synapse ID of project in which to create\
                                      file view')
    parser.add_argument('-n', '--name', help='Name of file view')
    parser.add_argument('-s', '--scopes',
                        help='A comma-delimited list of Synapse IDs of scopes\
                              that the file view should include.')
    parser.add_argument('--add_default_columns', action='store_true',
                        help='Add default columns to file view.')
    parser.add_argument('json', nargs='+',
                        help='One or more json files to use to define the file\
                              view Schema.')

    args = parser.parse_args()

    syn = synapseclient.login(silent=True)

    project_id = args.id
    scopes = args.scopes
    jsons = args.json
    view_name = args.name

    # get schema from json
    cols = []
    [cols.extend(createColumnsFromJson(j)) for j in jsons]

    scopes = scopes.split(',')
    fv = synapseclient.EntityViewSchema(name=view_name, parent=project_id,
                                        scopes=scopes, columns=cols,
                                        add_default_columns=args.add_default_columns)

    syn.store(fv)


if __name__ == '__main__':
    main()

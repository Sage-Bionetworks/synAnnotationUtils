#!/usr/bin/env python

'''
Create empty file view from a json of interest

'''

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
        new_path = urlparse.urljoin('file:', urllib.pathname2url(os.path.abspath(path)))
    else:
        new_path = path

    return new_path


def makeFileView(view_name, project_id, scopes, cols):

    entity_view = synapseclient.EntityViewSchema(name=view_name, parent=project_id,
                                                 scopes=scopes, columns=cols)

    return entity_view


def getSchemaFromJson(json_file):
    print json_file
    f = urllib.urlopen(path2url(json_file))
    data = json.load(f)

    cols = []

    for d in data:
        k = d['name']
        column_type = d['columnType']
        enumValues = [a['value'] for a in d['enumValues']]
        ms = d['maximumSize']
        cols.append(synapseclient.Column(name=k, columnType=column_type,
                                         enumValues=enumValues, maximumSize=ms))

    return cols


def main():
    import argparse
    syn = synapseclient.login(silent=True)

    parser = argparse.ArgumentParser(description="Create Empty File View")
    parser.add_argument('--id', help='Synapse ID of project in which to create\
    file view')
    parser.add_argument('--name', help='Name of file view')
    parser.add_argument('-s', '--scopes',
                        help='comma-delimited list of Synapse IDs of scope that file view should span')
    parser.add_argument('json', nargs='+', help='One or more json files to be used to\
    generate the file view')

    args = parser.parse_args()

    sys.stderr.write('Preparing to create fileview\n')
    project_id = args.id
    scopes = args.scopes
    jsons = args.json
    view_name = args.name

    # get schema from json
    cols = []
    [cols.extend(getSchemaFromJson(j)) for j in jsons]
#    print len(cols)
    fv = makeFileView(view_name, project_id, scopes.split(','), cols)

    syn.store(fv)


if __name__ == '__main__':
    main()

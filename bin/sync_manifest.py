'''
Example:
   Create manifest
   $ python sync_manifest.py -c -d ../../dir1/dir2 -id syn123456

   Upload using sync function
   $ python sync_manifest.py -u -m manifest.tsv
'''

import os
import sys
import json
import pandas as pd
import urlparse
import urllib
import synapseclient
import synapseutils as synu
from synapseclient import Folder

# walk through local directory
# get a list of dirs and a list of files
def _getLists(local_root):
    dir_list = []
    file_list = []

    local_dir = os.walk(local_root)

    for dirpath, dirnames, filenames in local_dir:
	    dirpath = os.path.abspath(dirpath)
	    dir_list.append(dirpath)
	    for name in filenames:
		    file_list.append(os.path.join(dirpath,name))
    return dir_list,file_list

# walk through Synapse
# update folders in Synapse to match the local dir
# get key-value pairs of dirname and synapse id
def _getSynapseDir(syn, synapse_id, local_root, dir_list):
    synapse_dir = {}

    synapse_root = syn.get(synapse_id)

    for (dirpath, dirpath_id), _, _ in synu.walk(syn,synapse_id):
        dirpath = dirpath.replace(synapse_root.name, os.path.abspath(local_root))
        synapse_dir[dirpath] = dirpath_id

    for di in dir_list:
        if not synapse_dir.has_key(di):
            new_folder = Folder(os.path.basename(di),synapse_dir[os.path.dirname(di)])		
            new_folder = syn.store(new_folder)
            synapse_dir[di] = new_folder.id
    return synapse_dir

# get a list of annotation keys
def _getAnnotationKey(dirs):
    key_list = ['used','executed']
    if dirs is not None:
        for di in dirs:
            if urlparse.urlparse(di).scheme != '':
                jfile = urllib.urlopen(di)
            else:
                jfile = open(di,'r')
            base,ext=os.path.splitext(os.path.basename(di))
            if ext=='.json':
                data=json.load(jfile)
            else:
                sys.stdout.write('File %s cannot be parsed. JSON format is required. \n' % di)
            data = pd.DataFrame(data)
            annotation_key = data['name']
            key_list = key_list + list(annotation_key)
    return key_list


# create manifest
def create(file_list,key_list,synapse_dir):
    result = pd.DataFrame()
    result['path'] = file_list
    result['parent'] = result['path'].map(lambda x: synapse_dir[os.path.dirname(x)])
    cols = list(result.columns)

    result = pd.concat([result,pd.DataFrame(columns=key_list)])
    result = result[cols + key_list] # reorder the columns

    result.to_csv('output.tsv',sep = '\t', index=False)
    sys.stdout.write('Manifest has been created. Stored as \"output.tsv\"\n')

def main():
    import argparse
    syn = synapseclient.login()
    
    parser = argparse.ArgumentParser(description="Create or upload sync manifest")
    parser.add_argument('-c','--create', help='create manifest', action='store_true')
    parser.add_argument('-u','--upload', help='sync to synapse using manifest', action='store_true')
    parser.add_argument('-d','--dir', help='local directory')
    parser.add_argument('-id','--id',help='Synapse ID of the project/file')
    parser.add_argument('-f','--files',
                        help='Path(s) to JSON file(s) of annotations. optional', nargs='+')
    parser.add_argument('-m','--manifest',help='manifest file')
    args=parser.parse_args()
    
    if args.create:
    	sys.stdout.write('Preparing to create manifest\n')
        local_root = args.dir
        synapse_id = args.id
        annotations = args.files
    
        dir_list, file_list = _getLists(local_root)
        synapse_dir = _getSynapseDir(syn, synapse_id,local_root,dir_list)
        key_list = _getAnnotationKey(annotations)

        create(file_list,key_list,synapse_dir)
    elif args.upload:
        sys.stdout.write('Preparing to upload files\n')
        synu.syncToSynapse(syn, args.manifest)
    else:
        sys.stdout.write('Please enter python sync_manifest.py -h for more information.\n')

if __name__ == '__main__':
	main()


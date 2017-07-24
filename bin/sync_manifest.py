'''
**Create sync manifest**

1. You can create a sync manifest by simply running 
   
   $ python sync_manifest.py -c -d home_dir_of_files/ -id syn1234567

    syn1234567 is the Synapse ID of a Synapse Project or a Synapse Folder that is the equivalent to home_dir_of_files in the directory hierarchy. 

    This will generate a tsv file lists out all the files under home_dir_of_files and create mirrored folder hierarchy on Synapse server at the same time. Please note no files has been uploaded at this time. 

2. Add annotation keys
   If you would like to add a set of annotation keys to the files, you can run
   
   $ python sync_manifest.py -c -d home_dir_of_files/ -id syn1234567 -f project_annotations.json

     We suggest you use the set of Sage standard annotation keys from our repo - https://raw.githubusercontent.com/Sage-Bionetworks/synapseAnnotations/master/synapseAnnotations/data/common/minimal_Sage_standard.json

   In this case, the script is 

   $ python sync_manifest.py -c -d home_dir_of_files/ -id syn1234567 -f https://raw.githubusercontent.com/Sage-Bionetworks/synapseAnnotations/master/synapseAnnotations/data/common/minimal_Sage_standard.json

3. Flatten folder hierarchy
   If you know your folder structure very well and would like to flatten the hierarchy, you can add another parameter '-n'.

   $ python sync_manifest.py -c -d home_dir_of_files/ -id syn1234567 -f https://raw.githubusercontent.com/Sage-Bionetworks/synapseAnnotations/master/synapseAnnotations/data/common/minimal_Sage_standard.json -n 2
   
   Once you set n=2, the folder with a depth greater than 2 will be flattened. 
   For example, if your folder structure on your local server is like below.
   
   --- home_dir_of_files
          --- rnaSeq
               --- sample1
                    --- file1.txt
                    --- file2.csv
               --- sample2 
          --- snpArray
               --- sample1
               --- sample2

  Since folder 'sample1' under 'rnaSeq' has a depth of 3, the files inside the folder will be renamed to "sample1_file1.txt" and "sample1_file2.csv" and to be placed under "rnaSeq" folder on the Synapse server.

**Upload and annotate files via sync function**

Once you made all the edits to your sync manifest (default manifest name is "output.tsv") to Synapse.

$ python sync_manifest.py -u -m output.tsv

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
def _getLists(local_root,depth):
    dir_list = []
    file_list = []

    for dirpath, _, filenames in os.walk(local_root):
        sub_dir = dirpath[len(local_root):]
        n = sub_dir.count(os.path.sep) + 1 if sub_dir != '' else 0
        dirpath = os.path.abspath(dirpath)
        if depth is not None:
            if n < depth:
                dir_list.append(dirpath)
        else:
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
                sys.stderr.write('File %s cannot be parsed. JSON format is required. \n' % di)
            data = pd.DataFrame(data)
            annotation_key = data['name']
            key_list = key_list + list(annotation_key)
    return key_list

def _getName(path, synapse_dir, local_root, depth):
    path_no_root = path[len(os.path.abspath(local_root)):]

    if depth is not None and path_no_root.count(os.path.sep) > depth-1:
        if str.startswith(path_no_root, '/'):
            path_no_root = path_no_root[1:]
        temp_name = path_no_root.split('/')[(depth-1):]
        name = '_'.join(temp_name)
    
        temp_name = '/'.join(temp_name)
        parent = synapse_dir[os.path.dirname(path[:-len(temp_name)])]
    else:
        name = os.path.basename(path)
        parent= synapse_dir[os.path.dirname(path)]
    return name,parent

# create manifest
def create(file_list,key_list,synapse_dir,local_root,depth):
    result = pd.DataFrame()
    result['path'] = file_list
    result['name'] = ""
    result['parent'] = ""
    
    for index, row in result.iterrows():
        row[['name','parent']] = _getName(row['path'], synapse_dir, local_root, depth)
        
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
    parser.add_argument('-i','--id',help='Synapse ID of the project/file')
    parser.add_argument('-f','--files',
                        help='Path(s) to JSON file(s) of annotations. optional', nargs='+')
    parser.add_argument('-n','--n', help='depth of hierarchy, DEFAULT is None', default=None)
    parser.add_argument('-m','--manifest',help='manifest file')
    args=parser.parse_args()
    
    if args.create:
        sys.stdout.write('Preparing to create manifest\n')
        local_root = args.dir
        synapse_id = args.id
        annotations = args.files
        depth = args.n

        if depth is not None:
            depth = int(depth) 

        dir_list, file_list = _getLists(local_root,depth)
        print(dir_list)
        synapse_dir = _getSynapseDir(syn, synapse_id,local_root,dir_list)
        key_list = _getAnnotationKey(annotations)

        create(file_list,key_list,synapse_dir,local_root,depth)
    elif args.upload:
        sys.stdout.write('Preparing to upload files\n')
        synu.syncToSynapse(syn, args.manifest)
    else:
        sys.stderr.write('Please enter python sync_manifest.py -h for more information.\n')

if __name__ == '__main__':
    main()


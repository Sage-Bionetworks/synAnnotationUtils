import synapseclient
syn = synapseclient.login()

import synapseutils as synu
import pandas as pd

def delAnnoByKey(syn,synId,keyList):
    """
    Delete annotations of a Synapse object by giving a list of keys. 
    :param syn:            A Synapse object: syn = synapseclient.login()- Must be logged into synapse
    :param synId:          A Synapse ID of Project, Folder, or File
    :param keyList:        A list of annotations keys that needs to be deleted
    Example:
       delAnnoByKey(syn,"syn12345",["dataType","projectName","sampleId"])
       
    """
    starting = syn.get(synId,downloadFile = False)
    if not is_container(starting):
        print "%s is a File" % synId
        _helperDelAnnoByKey(syn,starting,keyList)
    else:
        directory = synu.walk(syn,synId)
        for dirpath,dirname,filename in directory:
            for i in filename:
                temp = syn.get(i[1],downloadFile = False)
                print "Getting file %s ..." % i[1]
                _helperDelAnnoByKey(syn,temp,keyList)
        
def _helperDelAnnoByKey(syn,temp,keyList):
    print "Deleting annotations ..."
    annoDict = temp.annotations
    for key in keyList:
        if key in annoDict.keys():
            print "> %s" % key
            annoDict.pop(key)
    temp.annotations = annoDict
    temp = syn.store(temp,forceVersion = False)
    print ""

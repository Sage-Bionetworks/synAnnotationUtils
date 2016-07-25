import synapseclient
syn = synapseclient.login()

import synapseutils as synu

def delAnnoByKey(syn,synId,keyList):
    """
    Delete annotations by key for a Synapse object
    :param syn:            A Synapse object: syn = synapseclient.login()- Must be logged into synapse
    :param synId:          A Synapse ID of Project, Folder, or File
    :param keyList         A list of annotations keys that needs to be deleted
   
    Example:
    
       delAnnoByKey(syn,"syn12345",["dataType","projectName","sampleId"])
       
    """

    print "Delte entity annotations by key(s) - %s" % ", ".join(keyList)
    starting = syn.get(synId,downloadFile = False)
    if not is_container(starting):
        print "%s is a File \n" % synId
        _helperDelAnnoByKey(syn,starting,keyList)
    else:
        directory = synu.walk(syn,synId)
        for dirpath,dirname,filename in directory:
            for i in filename:
                temp = syn.get(i[1],downloadFile = False)
                print "Getting File %s ..." % i[1]
                _helperDelAnnoByKey(syn,temp,keyList)
        
def _helperDelAnnoByKey(syn,temp,keyList):
    annoDict = temp.annotations
    annoKeys = annoDict.keys()
    if any(x in keyList for x in annoKeys):
        print "Deleting annotations ..."
        for key in keyList:
            if key in annoDict.keys():
                print "> %s" % key
                annoDict.pop(key)
        temp.annotations = annoDict
        temp = syn.store(temp,forceVersion = False)
    else:
        print "Pass."
    print ""
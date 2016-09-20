import synapseclient
from synapseclient.entity import is_container
import synapseutils as synu

def delAnnoByKey(syn,synId,keyList):
    """
    Delete annotations by key for a Synapse object
    :param syn:            A Synapse object: syn = synapseclient.login()- Must be logged into synapse
    :param synId:          A Synapse ID of Project, Folder, or File or a list of Synapse IDs
    :param keyList         A list of annotations keys that needs to be deleted
   
    Example:
    
       delAnnoByKey(syn,"syn12345",["dataType","projectName","sampleId"])
       OR
       delAnnoByKey(syn,["syn1","syn2","syn3"],["dataType"])
       
    """

    print "Delte entity annotations by key(s) - \n %s" % "\n".join(keyList)
    
    if type(synId) is list:
        print "Input is a list of Synapse IDs \n"
        for synID in synId:
            print "Getting File %s ..." % synID
            temp = syn.get(synID,downloadFile = False)
            _helperDelAnnoByKey(syn,temp,keyList)
    else:
        print "Input is a Synpase ID \n"
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
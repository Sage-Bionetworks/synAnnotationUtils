import synapseclient
from synapseclient.entity import is_container
syn = synapseclient.login()

import synapseutils as synu

def auditFormatTypeByFileName(syn,synId,annoKey,annoDict):
    """
    Audit entity file type annotations by checking file name with file type annotation
    :param syn:            A Synapse object: syn = synapseclient.login()- Must be logged into synapse
    :param synId:          A Synapse ID of Project, Folder, or File
    :param annoKey:        The annotation key for file type. (i.e. "fileType", "fileFormat", or "formatType")
    :param annoDict        A dict where key is the extension of the filename, 
                           value is the corresponding file type value in entity annotations
    
    Return:
    
      If synId is an ID of a Project/Folder
        A dict with 3 keys and each value is a list of File Synapse ID 
        (i.e. {"incorrect":[], "missingInAnno":["syn1"], "missingInDict":["syn2","syn3"])
      If synId is an ID of a File
        A string with 4 possible answers:
            1. "correct"
            2. "incorrect"
            3. "missingInAnno" - no file type in entity annotations
            4. "missingInDict" - file type is not found in file type annoDict
        
    Example:
    
       result = auditFormatType(syn,"syn12345","fileType",{".bam":"bam", ".doc":"word", "bw":"bigwig"})
       
    """
    
    needAnnotated = {"incorrect":[],
                     "missingInAnno":[],
                     "missingInDict":[]}
    
    starting = syn.get(synId,downloadFile = False)
    if not is_container(starting):
        print "%s is a File \n" % synId
        _helperAuditFormatTypeByFileName(syn,starting,annoKey,annoDict,needAnnotated)
        result = "correct"
        for key in needAnnotated.keys(): 
            if len(needAnnotated[key]):
                result = key
        return result
    else:
        directory = synu.walk(syn,synId)
        for dirpath,dirname,filename in directory:
            for i in filename:
                temp = syn.get(i[1],downloadFile = False)
                print "Getting File %s ..." % i[1]
                _helperAuditFormatTypeByFileName(syn,temp,annoKey,annoDict,needAnnotated)
        return needAnnotated

def _helperAuditFormatTypeByFileName(syn,temp,annoKey,annoDict,needAnnotated):
    print "Checking %s..." % annoKey
    tempName = temp.name
    tempType = ""
    for ext in annoDict.keys():
        if tempName.endswith(ext):
            tempType = annoDict[ext]
            if annoKey in temp.annotations.keys():
                if temp[annoKey][0] != tempType:
                    needAnnotated["incorrect"].append(temp.id)
                    print ">Incorrect"
                else:
                    print ">Passed!"
            else:
                needAnnotated["missingInAnno"].append(temp.id)
                print "> Missing in entity annotations"
            break
    if tempType == "":
        needAnnotated["missingInDict"].append(temp.id)
        print "> Missing file types dictionary"
    print ""


def updateFormatTypeByFileName(syn,synId,annoKey,annoDict):
    """
    Audit entity file type annotations
    :param syn:            A Synapse object: syn = synapseclient.login()- Must be logged into synapse
    :param synId:          A Synapse ID of Project, Folder, or File OR a list of Synapse IDs of File
    :param annoKey:        The annotation key for file type. (i.e. "fileType", "fileFormat", or "formatType")
    :param annoDict        A dict where key is the extension of the filename, 
                           value is the corresponding file type value in entity annotations
        
    Example:
    
       updateFormatTypeByFileName(syn,"syn12345","fileType",{".bam":"bam", ".doc":"word", "bw":"bigwig"})
       OR
       updateFormatTypeByFileName(syn,["syn1","syn2"],"fileType",{".bam":"bam", ".doc":"word", "bw":"bigwig"})
       
    """
    if type(synId) is list:
        print "Input is a list of Synapse IDs \n"
        for synID in synId:
            print "Getting File %s ..." % synID
            temp = syn.get(synID,downloadFile = False)
            _helperUpdateFormatTypeByFileName(syn,temp,annoKey,annoDict)
    else:
        print "Input is a Synpase ID \n"
        starting = syn.get(synId,downloadFile = False)
        if not is_container(starting):
            print "%s is a File \n" % synId
            _helperUpdateFormatTypeByFileName(syn,starting,annoKey,annoDict)
        else:
            directory = synu.walk(syn,synId)
            for dirpath,dirname,filename in directory:
                for i in filename:
                    temp = syn.get(i[1],downloadFile = False)
                    print "Getting File %s ..." % i[1]
                    _helperUpdateFormatTypeByFileName(syn,temp,annoKey,annoDict)

def _helperUpdateFormatTypeByFileName(syn,temp,annoKey,annoDict):
    print "Updating %s..." % annoKey
    tempName = temp.name
    tempType = ""
    for ext in annoDict.keys():
        if tempName.endswith(ext):
            tempType = annoDict[ext]
            temp[annoKey] = tempType
            print "> Done!"
            break
    if tempType == "":
        print "> ERROR: file type not found in file types dictionary"
    print ""

def mergeDicts(dictList):
    """
    A helper function to merge multiple dicts
    :param syn:            A list of dicts 
    
    Return:
        A merged dict
        
    Example:
    
       result = mergeDicts([dict1,dict2,dict3])
       
    """
    result = {}
    for d in dictList:
        for k in d.keys():
            if k in result.keys():
                for v in d[k]:
                    result[k].append(v)
            else:
                result[k] = d[k]
    return result

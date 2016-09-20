import synapseclient
from synapseclient.entity import is_container
import synapseutils as synu

# Audit common dictionary
def auditCommonDict(syn, synId, annoDict):
    """
    Audit entity annotations against common dictionary shared among all enities
    :param syn:            A Synapse object: syn = synapseclient.login()- Must be logged into synapse
    :param synId:          A Synapse ID of Project, Folder, or File
    :param annoDict        A dict of annotations shared among entities
    
    A generator that contains:
      If synId is an ID of a Project/Folder
        entityMissAllAnno:     A list of Synapse IDs that have not been annotatd
        incorrectAnnoated:     A dict where key is the annotation key and value is a list of entities 
                               that were annotated incorrectly (i.e. {"sampleId":["syn1","syn2"]})
        missedAnno:            A dict where key is the annotation key and value is a list of entities
                               that were missing the annotation (i.e. {"dataType":["syn3","syn4"]})
      If synId is an ID of a File
        entityMissAllAnno:     A boolean if synID is a ID of a File
        incorrectAnnoated:     A list of keys that were annotated incorrectly
        missedAnno:            A list of keys were missing the entity annotation
        
    Example:
       result = auditCommonDict(syn,"syn12345",{"dataType":"testing","projectName":"foo"})
       entityMissAllAnno, incorrectAnnoated, missingAnno = result.next()

    """

    entityMissAllAnno = []
    incorrectAnnotated = {}
    missingAnno = {}
    print "Check annotations against common dictionary. \n"
    starting = syn.get(synId,downloadFile = False)
    if not is_container(starting):
        print "%s is a File \n" % synId
        _helperAuditCommonDict(syn,starting,annoDict,entityMissAllAnno,incorrectAnnotated,missingAnno)
        noAnno = False
        if len(entityMissAllAnno):
            noAnno = True
        yield noAnno,incorrectAnnotated.keys(),missingAnno.keys()
    else:
        directory = synu.walk(syn,synId)
        for dirpath,dirname,filename in directory:
            for i in filename:
                temp = syn.get(i[1],downloadFile = False)
                print "Getting file %s ..." % i[1]
                _helperAuditCommonDict(syn,temp,annoDict,entityMissAllAnno,incorrectAnnotated,missingAnno)
        yield entityMissAllAnno,incorrectAnnotated,missingAnno
                
def _helperAuditCommonDict(syn, temp, annoDict,entityMissAllAnno,incorrectAnnotated,missingAnno):
    print "Checking..."
    tempDict = temp.annotations

    if bool(tempDict):
        added, removed, modified = dict_compare(tempDict, annoDict)
        if len(added) > 0:
            print ">Keys: found ONLY in entity annotations"
            print ">>"+", ".join(str(x) for x in added) 
        if len(removed) > 0:
            for x in removed:
                annoKey = str(x)
                if annoKey in missingAnno.keys():
                    missingAnno[annoKey].append(temp)
                else:
                    missingAnno[annoKey] = [temp]
            print ">Keys: NOT found in entity annotations"
            print ">>"+", ".join(str(x) for x in removed)
        if len(modified) > 0:
            for x in modified:
                annoKey = str(x)
                if annoKey in incorrectAnnotated.keys():
                    incorrectAnnotated[annoKey].append(temp)
                else:
                    incorrectAnnotated[annoKey] = [temp]
            print ">Values: incorrect in entity annotations"
            print ">>"+", ".join(str(x) for x in modified)
        if len(added)+len(removed)+len(modified) == 0:
            print "Pass."
    else:
        print ">Annotations: missing"
        entityMissAllAnno.append(temp)
    print ""

def dict_compare(d1, d2):
    d1_keys = set(d1.keys())
    d2_keys = set(d2.keys())
    intersect_keys = d1_keys.intersection(d2_keys)
    added = d1_keys - d2_keys
    removed = d2_keys - d1_keys
    modified = {o : (d1[o], d2[o]) for o in intersect_keys if ''.join(d1[o]) != d2[o]}
    return added, removed, modified


# Update Annotations 
## by dict
def updateAnnoByDict(syn,synId,annoDict):
    """
    Update annotations by giving a dict
    :param syn:            A Synapse object: syn = synapseclient.login()- Must be logged into synapse
    :param synId:          A Synapse ID of Project, Folder, or File OR a list of Synpase Objects
    :param annoDict        A dict of annotations
    
    Example:

       updateAnnoByDict(syn,"syn12345",{"dataType":"testing","projectName":"foo"})
       OR
       updateAnnoByDict(syn,["syn1","syn2"],{"dataType":"testing","projectName":"foo"})
       
    """
    
    if type(synId) is list:
        print "Input is a list of Synapse Objects \n"
        for synID in synId:
            print "Getting File %s ..." % synID.id
            temp = synID
            _helperUpdateAnnoByDict(syn,temp,annoDict)
    else:
        print "Input is a Synpase ID \n"
        starting = syn.get(synId,downloadFile = False)
        if not is_container(starting):
            print "%s is a File \n" % synId
            _helperUpdateAnnoByDict(syn,starting,annoDict)
        else:
            directory = synu.walk(syn,synId)
            for dirpath,dirname,filename in directory:
                for i in filename:
                    temp = syn.get(i[1],downloadFile = False)
                    print "Getting File %s ..." % i[1]
                    _helperUpdateAnnoByDict(syn,temp,annoDict)

def _helperUpdateAnnoByDict(syn,temp,annoDict):
    print "> Updating annotations..."
    temp.annotations.update(annoDict)
    temp = syn.store(temp,forceVersion = False)
    print "Completed. \n"


## by idDict
def updateAnnoByIdDictFromDict(syn,idDict,annoDict):
    
    """
    Update annotations from dictionary
    by a given dict(key:annotation keys need to updated, value:a list of Synpase Objects)
    
    :param syn:            A Synapse object: syn = synapseclient.login()- Must be logged into synapse
    :param idDict:         A dict.Key is the annotations key, value is a list of Synapse IDs
    :param annoDict        A dict of annotations

    Example:

        updateAnnoByIdDictFromDict(syn,{"dataType":["syn1","syn2"]},{"dataType":"csv","test":"TRUE"})
       
    """
    for key in idDict:
        print "updating annotaion values for key: %s" % key
        for synId in idDict[key]:
            print "> %s" %synId
            temp = synId
            temp[key] = annoDict[key]
            temp = syn.store(temp,forceVersion = False)
        print ""
from collections import defaultdict

# Audit common dictionary
def auditCommonDict(syn, synId, commonDict):
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
    incorrectAnnotated = defaultdict(list)
    missingAnno = defaultdict(list)
    print "Check annotations against common dictionary. \n"
    synObj = syn.get(synId,downloadFile = False)
    if not is_container(synObj):
        print "%s is a File \n" % synId
        tempEntityMissAllAnno,tempIncorrectAnnotated,tempMissingAnno = _helperAuditCommonDict(syn,synObj,annoDict)
        entityMissAllAnno.append(tempEntityMissAllAnno)
        incorrectAnnotated.update(tempIncorrectAnnotated)
        missingAnno.update(tempMissingAnno)
    else:
        directory = synu.walk(syn,synId)
        for dirpath,dirname,filename in directory:
            for i in filename:
                ent = syn.get(i[1],downloadFile = False)
                print "Getting file %s ..." % i[1]
                tempEntityMissAllAnno,tempIncorrectAnnotated,tempMissingAnno = _helperAuditCommonDict(syn,synObj,annoDict)
                entityMissAllAnno.append(tempEntityMissAllAnno)
                incorrectAnnotated.update(tempIncorrectAnnotated)
                missingAnno.update(tempMissingAnno)
    
    yield entityMissAllAnno,incorrectAnnotated,missingAnno
                
def _helperAuditCommonDict(syn, synEntity, commonDict):
    print "Checking..."

    entityMissAllAnno = []
    incorrectAnnotated = defaultdict(list)
    missingAnno = defaultdict(list)
    
    if synEntity.annotations:
        added, removed, modified = dict_compare(synEntity.annotations, commonDict)
        # Check if any annotation keys only found in entity annotations
        if added > 0:
            print ">Keys: found ONLY in entity annotations"
            print ">>"+", ".join(str(x) for x in added) 
        # Check if any annotation keys are not found in entity annotations
        if removed > 0:
            for x in removed:
                missingAnno[x].append(synEntity)
            print ">Keys: NOT found in entity annotations"
            print ">>"+", ".join(str(x) for x in removed)
        # Check if any annotation keys were annotated incorrectly
        if modified > 0:
            for x in modified:
                incorrectAnnotated[x].append(synEntity)
            print ">Values: incorrect in entity annotations"
            print ">>"+", ".join(str(x) for x in modified)
        if len(added)+len(removed)+len(modified) == 0:
            print "Pass."
    else:
        print ">Annotations: missing"
        entityMissAllAnno.append(synEntity)
    print ""
    return entityMissAllAnno,incorrectAnnotated,missingAnno

def dict_compare(d1, d2):
    d1_keys = set(d1.keys())
    d2_keys = set(d2.keys())
    intersect_keys = d1_keys.intersection(d2_keys)
    added = d1_keys - d2_keys
    removed = d2_keys - d1_keys
    modified = {o : (d1[o], d2[o]) for o in intersect_keys if ''.join(d1[o]) != d2[o]}
    return added, removed, modified

def auditAgainstMetadata(syn, synId, metaDf, refCol, cols2Check,fileExts):
    """
    Audit entity annotations against metadata
    :param syn:            A Synapse object: syn = synapseclient.login()- Must be logged into synapse
    :param synId:          A Synapse ID of Project, Folder, or File
    :param metaDf          A pandas data frame of entity metadata
    :param refCol          A name of the column in metaDf that matching one of the entity attributes
    :param cols2Check      A list of columns in metaDf need to be audited with entity annotations 
    :param fileExts        A list of all file extensions (PsychENCODE ONLY!!!) 
    
    A generator that contains:
      If synId is an ID of a Project/Folder
        entityMissMetadata:    A list of Synapse IDs that have no matching metadata
        incorrectAnnoated:     A dict object where key is the annotation key and value is a list of entities 
                               that were annotated incorrectly (i.e. {"sampleId":["syn1","syn2"]})
        missedAnno:            A dict object where key is the annotation key and value is a list of entities
                               that were missing the annotation (i.e. {"dataType":["syn3","syn4"]})
      If synId is an ID of a File
        entityMissMetadata:    A boolean. True when the entity has no matching metadata
        incorrectAnnoated:     A list of keys that were annotated incorrectly
        missedAnno:            A list of keys were not annotated
        
    Example:
       result = auditAgainstMetadata(syn,"syn12345",metadata,"id",["dataType","tester"],[".bam",".csv"])
       entityMissMetadata,incorrectAnnotated, missingAnno = result.next()
       
    """
    entityMissMetadata = []
    incorrectAnnotated = defaultdict(list)
    missingAnno = defaultdict(list)
    print "Check annotations against metadata.\n"
    starting = syn.get(synId,downloadFile = False)
    if not is_container(starting):
        print "%s is a File \n" % synId
        _helperAuditMetadata(syn,starting,metaDf,refCol,cols2Check,fileExts,
                             entityMissMetadata,incorrectAnnotated,missingAnno)
        noMeta = False
        if len(entityMissMetadata):
            noMeta = True
        yield noMeta,incorrectAnnotated.keys(),missingAnno.keys()
    else:
        directory = synu.walk(syn,synId)
        for dirpath,dirname,filename in directory:
            for i in filename:
                temp = syn.get(i[1],downloadFile = False)
                print "Getting File %s ..." % i[1]
                _helperAuditMetadata(syn,temp,metaDf,refCol,cols2Check,fileExts,
                                     entityMissMetadata,incorrectAnnotated,missingAnno)
        yield entityMissMetadata,incorrectAnnotated,missingAnno

def _helperAuditMetadata(syn,temp,metaDf,refCol,cols2Check,fileExts,
                         entityMissMetadata,incorrectAnnotated,missingAnno):
    """
    This helper function is built for PsychENCODE project data release. 
    The entity name without extension is map to a column in the metadata data frame
       
    """
    
    print "Checking annotations against metadata..."
    tempDict = temp.annotations
    
    exts = ')|('.join(fileExts)
    exts = r'(' + exts + ')'
    tempName = re.sub(exts,"",temp.name)
    
    if bool(tempDict):
        row = metaDf.loc[metaDf[refCol] == tempName]
        if row.empty:
            entityMissMetadata.append(temp)
            print "missing metadata"
        else:
            for colName in cols2Check:
                print ">%s checking..." % colName
                if colName in tempDict.keys():
                    if map(str,row[colName])[0] != temp[colName][0]:
                        incorrectAnnotated[colName].append(temp)
                        print ">>incorrect"
                    else:
                        print ">>Passed!"
                else:
                    missingAnno[colName].append(temp)
                    print ">>missing"
    print ""

def auditFormatTypeByFileName(syn,synId,annoKey,annoDict):
    """
    Audit entity file type annotations by checking file name with file type annotation
    :param syn:            A Synapse object: syn = synapseclient.login()- Must be logged into synapse
    :param synId:          A Synapse ID of Project, Folder, or File
    :param annoKey:        The annotation key for file type. (i.e. "fileType", "fileFormat", or "formatType")
    :param annoDict        A dict where key is the extension of the filename, 
                           value is the corresponding file type value in entity annotations
    
    A generator that contains:
    
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
       result = result.next()

    """
    
    needAnnotated = defaultdict(list)

    #{"incorrect":[],
    # "missingInAnno":[],
    # "missingInDict":[]}
    
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
                    needAnnotated["incorrect"].append(temp)
                    print ">Incorrect"
                else:
                    print ">Passed!"
            else:
                needAnnotated["missingInAnno"].append(temp)
                print "> Missing in entity annotations"
            break
    if tempType == "":
        needAnnotated["missingInDict"].append(temp)
        print "> Missing file types dictionary"
    print ""

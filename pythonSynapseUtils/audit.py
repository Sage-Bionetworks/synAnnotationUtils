from collections import defaultdict
import logging

# Audit common dictionary
def auditCommonDict(syn, synId, commonDict):
    """
    Audit entity annotations against common dictionary shared among all enities
    :param syn:            A Synapse object: syn = synapseclient.login()- Must be logged into synapse
    :param synId:          A Synapse ID of Project, Folder, or File
    :param annoDict        A dict of annotations shared among entities
    
    A generator that contains:
        entityMissAllAnno:     A list of Synapse IDs that have not been annotatd
        incorrectAnnoated:     A dict where key is the annotation key and value is a list of entities 
                               that were annotated incorrectly (i.e. {"sampleId":["syn1","syn2"]})
        missedAnno:            A dict where key is the annotation key and value is a list of entities
                               that were missing the annotation (i.e. {"dataType":["syn3","syn4"]})
        
    Example:
       result = auditCommonDict(syn,"syn12345",{"dataType":"testing","projectName":"foo"})
       entityMissAllAnno, incorrectAnnoated, missingAnno = result.next()

    """

    entityMissAllAnno = []
    incorrectAnnotated = defaultdict(list)
    missingAnno = defaultdict(list)
    logging.info("Check annotations against common dictionary.")
    synObj = syn.get(synId,downloadFile = False)
    if not is_container(synObj):
        logging.info("%s is a File" % synId)
        tempEntityMissAllAnno,tempIncorrectAnnotated,tempMissingAnno = _helperAuditCommonDict(syn,synObj,annoDict)
        entityMissAllAnno.append(tempEntityMissAllAnno)
        incorrectAnnotated.update(tempIncorrectAnnotated)
        missingAnno.update(tempMissingAnno)
    else:
        directory = synu.walk(syn,synId)
        for dirpath,dirname,filename in directory:
            for i in filename:
                ent = syn.get(i[1],downloadFile = False)
                logging.info("Getting file %s ..." % i[1])
                tempEntityMissAllAnno,tempIncorrectAnnotated,tempMissingAnno = _helperAuditCommonDict(syn,synObj,annoDict)
                entityMissAllAnno.append(tempEntityMissAllAnno)
                incorrectAnnotated.update(tempIncorrectAnnotated)
                missingAnno.update(tempMissingAnno)
    
    yield entityMissAllAnno,incorrectAnnotated,missingAnno
                
def _helperAuditCommonDict(syn, synEntity, commonDict):
    logging.info("Checking...")

    entityMissAllAnno = []
    incorrectAnnotated = defaultdict(list)
    missingAnno = defaultdict(list)
    
    if synEntity.annotations:
        novel, missing, modified = dict_compare(synEntity.annotations, commonDict)
        # Check if any annotation keys only found in entity annotations
        if novel > 0:
            logging.info("Keys: found ONLY in entity annotations")
            logging.info(", ".join(str(x) for x in novel))
        # Check if any annotation keys are not found in entity annotations
        if missing > 0:
            for x in missing:
                missingAnno[x].append(synEntity)
            logging.info("Keys: NOT found in entity annotations")
            logging.info(", ".join(str(x) for x in missing))
        # Check if any annotation keys were annotated incorrectly
        if modified > 0:
            for x in modified:
                incorrectAnnotated[x].append(synEntity)
            logging.info("Values: incorrect in entity annotations")
            logging.info(", ".join(str(x) for x in modified))
        if len(added)+len(removed)+len(modified) == 0:
            logging.info("Pass.")
    else:
        logging.info("Annotations: missing")
        entityMissAllAnno.append(synEntity)
    return entityMissAllAnno,incorrectAnnotated,missingAnno

def dict_compare(d1, d2):
    d1_keys = set(d1.keys())
    d2_keys = set(d2.keys())
    intersect_keys = d1_keys.intersection(d2_keys)
    novel = d1_keys - d2_keys
    missing = d2_keys - d1_keys
    modified = [o for o in intersect_keys if ''.join(d1[o]) != d2[o]]
    return added, missing, modified

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
        
    Example:
       result = auditAgainstMetadata(syn,"syn12345",metadata,"id",["dataType","tester"],[".bam",".csv"])
       entityMissMetadata,incorrectAnnotated, missingAnno = result.next()
       
    """
    entityMissMetadata = []
    incorrectAnnotated = defaultdict(list)
    missingAnno = defaultdict(list)
    logging.info("Check annotations against metadata.")
    synEntity = syn.get(synId,downloadFile = False)
    if not is_container(synEntity):
        logging.info("%s is a File" % synId)
        tempEntityMissMetadata,tempIncorrectAnnotated,tempMissingAnno = _helperAuditMetadata(syn,synEntity,metaDf,refCol,cols2Check,fileExts)
        entityMissMetadata.append(tempEntityMissMetadata)
        incorrectAnnotated.update(tempIncorrectAnnotated)
        missingAnno.update(tempMissingAnno)
    else:
        directory = synu.walk(syn,synId)
        for dirpath,dirname,filename in directory:
            for i in filename:
                synEntity = syn.get(i[1],downloadFile = False)
                logging.info("Getting File %s ..." % i[1])
                tempEntityMissMetadata,tempIncorrectAnnotated,tempMissingAnno = _helperAuditMetadata(syn,synEntity,metaDf,refCol,cols2Check,fileExts)
                ntityMissMetadata.append(tempEntityMissMetadata)
                incorrectAnnotated.update(tempIncorrectAnnotated)
                missingAnno.update(tempMissingAnno)

    yield entityMissMetadata,incorrectAnnotated,missingAnno

def _helperAuditMetadata(syn,synEntity,metaDf,refCol,cols2Check,fileExts):
    """
    This helper function is built for PsychENCODE project data release. 
    The entity name without extension is map to a column in the metadata data frame
       
    """
    
    logging.info("Checking annotations against metadata...")

    entityMissMetadata = []
    incorrectAnnotated = defaultdict(list)
    missingAnno = defaultdict(list)

    entityDict = synEntity.annotations
    
    exts = ')|('.join(fileExts)
    exts = r'(' + exts + ')'
    entityName = re.sub(exts,"",synEntity.name)
    
    if entityDict:
        row = metaDf.loc[metaDf[refCol] == entityName]
        if row.empty:
            entityMissMetadata.append(synEntity)
            logging.info("missing metadata")
        else:
            for colName in cols2Check:
                logging.info("%s checking..." % colName)
                if colName in entityDict.keys():
                    if map(str,row[colName])[0] != synEntity[colName][0]:
                        incorrectAnnotated[colName].append(synEntity)
                        logging.info("incorrect")
                    else:
                        logging.info("Passed!")
                else:
                    missingAnno[colName].append(synEntity)
                    logging.info("missing")
    return entityMissMetadata,incorrectAnnotated,missingAnno

def auditFormatTypeByFileName(syn,synId,annoKey,annoDict):
    """
    Audit entity file type annotations by checking file name with file type annotation
    :param syn:            A Synapse object: syn = synapseclient.login()- Must be logged into synapse
    :param synId:          A Synapse ID of Project, Folder, or File
    :param annoKey:        The annotation key for file type. (i.e. "fileType", "fileFormat", or "formatType")
    :param annoDict        A dict where key is the extension of the filename, 
                           value is the corresponding file type value in entity annotations
    
    A generator that contains:
        A dict with 3 keys and each value is a list of File Synapse ID 
        (i.e. {"incorrect":[], "missingInAnno":["syn1"], "missingInDict":["syn2","syn3"])
        
    Example:
    
       result = auditFormatType(syn,"syn12345","fileType",{".bam":"bam", ".doc":"word", "bw":"bigwig"})
       result = result.next()

    """
    
    auditResult = defaultdict(list)
    
    synEntity = syn.get(synId,downloadFile = False)
    if not is_container(synEntity):
        logging.info("%s is a File" % synId)
        tempAuditResult = _helperAuditFormatTypeByFileName(syn,synEntity,annoKey,annoDict)
        auditResult.update(tempAuditResult)

    else:
        directory = synu.walk(syn,synId)
        for dirpath,dirname,filename in directory:
            for i in filename:
                temp = syn.get(i[1],downloadFile = False)
                logging.info("Getting File %s ..." % i[1])
                tempAuditResult = _helperAuditFormatTypeByFileName(syn,synEntity,annoKey,annoDict)
                auditResult.update(tempAuditResult)
    yield needAnnotated

def _helperAuditFormatTypeByFileName(syn,synEntity,annoKey,annoDict):
    logging.info("Checking %s..." % annoKey)
    auditResult = defaultdict(list)

    entityName = synEntity.name
    entityType = ""
    for ext in annoDict.keys():
        if entityName.endswith(ext):
            entityType = annoDict[ext]
            if annoKey in synEntity.annotations.keys():
                if synEntity[annoKey][0] != entityType:
                    auditResult["incorrect"].append(synEntity)
                    logging.info("Incorrect")
                else:
                    logging.info("Passed!")
            else:
                auditResult["missingInAnno"].append(synEntity)
                logging.info("Missing in entity annotations")
            break
    if entityType == "":
        auditResult["missingInDict"].append(synEntity)
        logging.info("Missing file types dictionary")
    return auditResult

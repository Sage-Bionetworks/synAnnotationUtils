import synapseclient
from synapseclient.entity import is_container
syn = synapseclient.login()

import synapseutils as synu
import pandas as pd
import re

#Audit
def auditAgainstMetadata(syn, synId, metaDf, refCol, cols2Check,fileExts):
    """
    Audit entity annotations against metadata
    :param syn:            A Synapse object: syn = synapseclient.login()- Must be logged into synapse
    :param synId:          A Synapse ID of Project, Folder, or File
    :param metaDf          A pandas data frame of entity metadata
    :param refCol          A name of the column in metaDf that matching one of the entity attributes
    :param cols2Check      A list of columns in metaDf need to be audited with entity annotations 
    :param fileExts        A list of all file extensions (PsychENCODE ONLY!!!) 
    
    Return:
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
       entityMissMetadata,incorrectAnnotated, missingAnno = 
               auditAgainstMetadata(syn,"syn12345",metadata,"id",["dataType","tester"],[".bam",".csv"])
       
    """
    entityMissMetadata = []
    incorrectAnnotated = {}
    missingAnno = {}
    print "Check annotations against metadata.\n"
    starting = syn.get(synId,downloadFile = False)
    if not is_container(starting):
        print "%s is a File \n" % synId
        _helperAuditMetadata(syn,starting,metaDf,refCol,cols2Check,fileExts,
                             entityMissMetadata,incorrectAnnotated,missingAnno)
        noMeta = False
        if len(entityMissMetadata):
            noMeta = True
        return noMeta,incorrectAnnotated.keys(),missingAnno.keys()
    else:
        directory = synu.walk(syn,synId)
        for dirpath,dirname,filename in directory:
            for i in filename:
                temp = syn.get(i[1],downloadFile = False)
                print "Getting File %s ..." % i[1]
                _helperAuditMetadata(syn,temp,metaDf,refCol,cols2Check,fileExts,
                                     entityMissMetadata,incorrectAnnotated,missingAnno)
        return entityMissMetadata,incorrectAnnotated,missingAnno

def _helperAuditMetadata(syn,temp,metaDf,refCol,cols2Check,fileExts,
                         entityMissMetadata,incorrectAnnotated,missingAnno):
    """
    This helper function is built for PsychENCODE project data release. 
    The entity name without extension is map to a column in the metadata data frame
       
    """
    
    print "Checking annotations against metadata..."
    tempDict = temp.annotations
    tempId = temp.id
    exts = ')|('.join(fileExts)
    exts = r'(' + exts + ')'
    tempName = re.sub(exts,"",temp.name)
    
    if bool(tempDict):
        row = metaDf.loc[metaDf[refCol] == tempName]
        if row.empty:
            entityMissMetadata.append(tempId)
            print "missing metadata"
        else:
            for colName in cols2Check:
                print ">%s checking..." % colName
                if colName in tempDict.keys():
                    if map(str,row[colName])[0] != temp[colName][0]:
                        if colName in incorrectAnnotated.keys():
                            incorrectAnnotated[colName].append(tempId)
                        else:
                            incorrectAnnotated[colName] = [tempId]
                        print ">>incorrect"
                    else:
                        print ">>Passed!"
                else:
                    if colName in missingAnno.keys():
                        missingAnno[colName].append(tempId)
                    else:
                        missingAnno[colName] = [tempId]
                    print ">>missing"
    print ""


#Update
#1
def updateAnnoByMetadata(syn, synId, metaDf, refCol, cols2Add,fileExts):
    """
    Audit entity annotations against metadata
    :param syn:            A Synapse object: syn = synapseclient.login()- Must be logged into synapse
    :param synId:          A Synapse ID of Project, Folder, or File OR a list of Synpase IDs of File
    :param metaDf          A pandas data frame of entity metadata
    :param refCol          A name of the column in metaDf that matching one of the entity attributes
    :param cols2Add        A list of columns in metaDf need to be added as entity annotations 
    :param fileExts        A list of all file extensions (PsychENCODE ONLY!!!) 
        
    Example:
    
        updateAnnoByMetadata(syn,"syn12345",metadata,"id",["dataType","tester"],[".bam",".csv"])
       
    """
    
    if type(synId) is list:
        print "Input is a list of Synapse IDs \n"
        for synID in synId:
            print "Getting File %s ..." % synID
            temp = syn.get(synID,downloadFile = False)
            _helperUpdateAnnoByMetadata(syn,temp,metaDf,refCol,cols2Add,fileExts)
    else:
        print "Input is a Synpase ID \n"
        starting = syn.get(synId,downloadFile = False)
        if not is_container(starting):
            print "%s is a File \n" % synId
            _helperUpdateAnnoByMetadata(syn,starting,metaDf,refCol,cols2Add,fileExts)
        else:
            directory = synu.walk(syn,synId)
            for dirpath,dirname,filename in directory:
                for i in filename:
                    temp = syn.get(i[1],downloadFile = False)
                    print "Getting File %s ..." % i[1]
                    _helperUpdateAnnoByMetadata(syn,temp,metaDf,refCol,cols2Add,fileExts)

def _helperUpdateAnnoByMetadata(syn,temp,metaDf,refCol,cols2Add,fileExts):
    
    """
    This helper function is built for PsychENCODE project data release. 
    The entity name without extension is map to a column in the metadata data frame
       
    """
    
    print "Updating annotations by metadata..."
    
    exts = ')|('.join(fileExts)
    exts = r'(' + exts + ')'
    tempName = re.sub(exts,"",temp.name)
    
    row = metaDf.loc[metaDf[refCol] == tempName]
    if row.empty:
        print "missing metadata"
    else:
        for colName in cols2Add:
            print "> %s " % colName
            temp[colName] = map(str,row[colName])[0]
        temp = syn.store(temp, forceVersion = False)
    print ""


#2
def updateAnnoByIdDictFromMeta(syn,idDict,metaDf,refCol,fileExts):
    
    """
    Update annotations from metadata 
    by a given dict(key:annotation keys need to updated, value:a list of Synpase IDs)
    
    :param syn:            A Synapse object: syn = synapseclient.login()- Must be logged into synapse
    :param idDict:         A dict.Key is the annotations key, value is a list of Synapse IDs
    :param metaDf          A pandas data frame of entity metadata
    :param refCol          A name of the column in metaDf that matching one of the entity attributes
    :param fileExts        A list of all file extensions (PsychENCODE ONLY!!!) 
    
    This function is built for PsychENCODE project data release. 
    The entity name without extension is map to a column in the metadata data frame
       
    """
    for key in idDict:
        print "updating annotaion values for key: %s" % key
        for synId in idDict[key]:
            print "> %s" %synId
            temp = syn.get(synId, downloadFile = False)
            exts = ')|('.join(fileExts)
            exts = r'(' + exts + ')'
            tempName = re.sub(exts,"",temp.name)
            row = df.loc[df[refCol] == tempName]
            temp[key] = map(str,row[key])[0]
            temp = syn.store(temp,forceVersion = False)
        print ""


# Search function
def searchInMetadata(syn, synId, metaDf, refCol,col2Check,values2Check,fileExts):
    """
    Search for a list of Synapse IDs with a given column and expected values
    :param syn:            A Synapse object: syn = synapseclient.login()- Must be logged into synapse
    :param synId:          A Synapse ID of Project, Folder
    :param metaDf          A pandas data frame of entity metadata
    :param refCol          A name of the column in metaDf that matching one of the entity attributes
    :param col2Check       A name of the column in metaDf you are seaching
    :param values2Check    A list of values you are searching
    :param fileExts        A list of all file extensions (PsychENCODE ONLY!!!) 
    
    Return:
      A list of Synapse IDs
        
    Example:
       IDs = searchInMetadata(syn,"syn123",metadata,"id","tester",["foo","bar"],[".bam",".csv"])
       
    """
    synapseIds = []
    print "Search in metadata for key: %s \n" % col2Check
    starting = syn.get(synId,downloadFile = False)
    if not is_container(starting):
        print "ERROR: %s is not a Synapse ID of Project or Folder" % synId
    else:
        directory = synu.walk(syn,synId)
        for dirpath,dirname,filename in directory:
            for i in filename:
                temp = syn.get(i[1],downloadFile = False)
                print "Getting File %s ..." % i[1]
                _helperSearchInMetadata(syn,temp,metaDf,refCol,col2Check,values2Check,fileExts,synapseIds)
                                    
        return synapseIds

def _helperSearchInMetadata(syn, temp,metaDf, refCol,col2Check,values2Check,fileExts,synapseIds):
    """
    This helper function is built for PsychENCODE project data release. 
    The entity name without extension is map to a column in the metadata data frame
       
    """
    
    print "Searching in metadata..."
    tempDict = temp.annotations
    tempId = temp.id
    exts = ')|('.join(fileExts)
    exts = r'(' + exts + ')'
    tempName = re.sub(exts,"",temp.name)
    
    if bool(tempDict):
        row = metaDf.loc[metaDf[refCol] == tempName]
        if row.empty:
            print "missing metadata"
        else:
            if map(str,row[col2Check])[0] in values2Check:
                synapseIds.append(tempId)
                print ">>Found!"
            else:
                print ">>Passed."
    print ""
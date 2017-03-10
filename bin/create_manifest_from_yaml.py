import yaml
import pandas as pd
import re
import synapseclient
from synapseclient.entity import is_container
syn = synapseclient.login()
import synapseutils as synu
import collections
from collections import defaultdict

def _updateEntityDict(fileName,newAnnotations,patterns):
    entityDict = defaultdict()
    if patterns is not None:
        for key, value in patterns.iteritems():
            regex = re.compile(key)
            if regex.search(fileName):
                entityDict.update(value)
    if newAnnotations is not None:
        entityDict.update(newAnnotations)
    return entityDict

def updateEntity(synId,newAnnotations,patterns):
    tempDict = defaultdict(dict)
    if newAnnotations is not None or patterns is not None:
        synapseObj = syn.get(synId,downloadFile = False)
        if not is_container(synapseObj):
            entityDict = _updateEntityDict(synapseObj.name,newAnnotations,patterns)
            tempDict[synId].update(entityDict)
        else:
            directory = synu.walk(syn,synId)
            for dirpath,dirname,filename in directory:
                for i in filename:
                    entityDict = _updateEntityDict(i[0],newAnnotations,patterns)
                    tempDict[i[1]].update(entityDict)
    return tempDict

def _updateMultiAnnoEntity(synId,multipleAnnotations,resultDict):
    entityAnnoList = []
    for annotations in multipleAnnotations:
        tempDict = {"synapseId":synId}
        tempDict.update(annotations)
        tempDict.update(resultDict[synId])
        entityAnnoList.append(tempDict)
    return entityAnnoList


def updateMultipleAnnoList(synId,multipleAnnotations,resultDict):
    multipleAnnoList = []
    multiAnnoIds = []
    synapseObj = syn.get(synId,downloadFile = False)
    if not is_container(synapseObj):
        entityAnnoList = _updateMultiAnnoEntity(synId, multipleAnnotations,resultDict)
        multipleAnnoList += entityAnnoList
        multiAnnoIds.append(synId)
    else:
        directory = synu.walk(syn,synId)
        for dirpath,dirname,filename in directory:
            for i in filename:
                entityAnnoList = _updateMultiAnnoEntity(i[1],multipleAnnotations,resultDict)
                multipleAnnoList += entityAnnoList
                multiAnnoIds.append(i[1])
    return multipleAnnoList, multiAnnoIds

def updateNestDicts(originalDict, newValDict):
    for k, v in newValDict.iteritems():
        if isinstance(v, collections.Mapping):
            r = updateNestDicts(originalDict.get(k, {}), v)
            originalDict[k] = r
        else:
            originalDict[k] = newValDict[k]
    return originalDict

def main():
    import argparse
    
    parser = argparse.ArgumentParser("Create manifest using scheme in yaml format")
    parser.add_argument("-in","--input" , help="Filename of scheme in yaml format",type=str)
    parser.add_argument("-out", "--output", help="Filename of manifest csv format",type=str)
    
    args = parser.parse_args()
    
    # Step 1: load the scheme
    print("Load the yaml scheme")
    with open(args.input) as f:
        scheme = yaml.load(f)
    
    resultDict = defaultdict(dict)
    multipleAnnoList = []
    multiAnnoIds = []
    
    # Step 2: Find the project id, 
    #       walk through the project and create a dictionary where keys are the synapse IDs of files
    print("Update common annotations")
    projectInfo = scheme["projectInfo"]
    newAnnotations = projectInfo.get('annotations')
    patterns = projectInfo.get("patternToMatch")
    
    tempResultDict = updateEntity(projectInfo['synId'],newAnnotations,patterns)
    resultDict.update(tempResultDict)

    # Step 3a: Get a list of entityIds
    print("Update entity annotations")
    regex = re.compile("^entityId\d{1,2}$")
    entityIds = [i for i in scheme.keys() if regex.search(i)]
    
    # Step 3b: Update the resultDict with entity info
    for entityId in entityIds:
        entityInfo = scheme[entityId]
    
        newAnnotations = entityInfo.get('annotations')
        patterns = entityInfo.get("patternToMatch")
    
        for synId in entityInfo['synId']:
            tempResultDict = updateEntity(synId,newAnnotations,patterns)
            resultDict = updateNestDicts(resultDict, tempResultDict)

        regex = re.compile("^annotations\d{1,2}$")
        multipleAnnotations = [v for k,v in entityInfo.iteritems() if regex.search(k)]
        if multipleAnnotations:
            for synId in entityInfo['synId']:
                tempDictList, tempIdList = updateMultipleAnnoList(synId,multipleAnnotations,resultDict)
                multipleAnnoList += tempDictList
                multiAnnoIds += tempIdList  
    
    # Step 4a: Get a list of filePaths
    regex = re.compile("^filePath\d{1,2}$")
    filePaths = [i for i in scheme.keys() if regex.search(i)]
    
    # Step 4b: Update the resultDict with path info
    for filePath in filePaths:
        pathInfo = scheme[filePath]
    
        newAnnotations = pathInfo.get('annotations')
        patterns = pathInfo.get("patternToMatch")
    
        for path in pathInfo['path']:
            projectDir = synu.walk(syn,projectInfo["synId"])
            for dirpath,dirname,filename in projectDir:
                if path == dirpath[0]:
                    tempResultDict = updateEntity(dirpath[1],newAnnotations,patterns)
                    resultDict = updateNestDicts(resultDict, tempResultDict)
    
    # Final Step: Convert dict of dict to a table
    print("Create manifest")
    for synId in multiAnnoIds:
        resultDict.pop(synId,None)
        
    resultDf = pd.DataFrame.from_dict(resultDict,orient="index")
    resultDf['synapseId'] = resultDf.index

    if multipleAnnoList:
        multiAnnoDf = pd.DataFrame(multipleAnnoList)
        resultDf = resultDf.append(multiAnnoDf)
    
    cols = list(resultDf)
    cols.insert(0, cols.pop(cols.index('synapseId')))
    resultDf = resultDf.ix[:, cols]

    resultDf.to_csv(args.output,index=False)

if __name__ == "__main__":
    main()

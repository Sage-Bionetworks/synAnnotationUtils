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
            temp = synID
            print "Accessing File %s ..." % temp.id
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
        for synObj in idDict[key]:
            print "> %s" %synId
            synObj[key] = annoDict[key]
            synObj = syn.store(synObj,forceVersion = False)
        print ""

def updateAnnoByMetadata(syn, synId, metaDf, refCol, cols2Add,fileExts):
    """
    Audit entity annotations against metadata
    :param syn:            A Synapse object: syn = synapseclient.login()- Must be logged into synapse
    :param synId:          A Synapse ID of Project, Folder, or File OR a list of Synpase Objects
    :param metaDf          A pandas data frame of entity metadata
    :param refCol          A name of the column in metaDf that matching one of the entity attributes
    :param cols2Add        A list of columns in metaDf need to be added as entity annotations 
    :param fileExts        A list of all file extensions (PsychENCODE ONLY!!!) 
        
    Example:
    
        updateAnnoByMetadata(syn,"syn12345",metadata,"id",["dataType","tester"],[".bam",".csv"])
       
    """
    
    if type(synId) is list:
        print "Input is a list of Synapse Objects \n"
        for synID in synId:
            temp = synID
            print "Accessing File %s ..." % temp.id
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

def updateAnnoByIdDictFromMeta(syn,idDict,metaDf,refCol,fileExts):
    
    """
    Update annotations from metadata 
    by a given dict(key:annotation keys need to updated, value:a list of Synpase Objects)
    
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
            temp = synId
            exts = ')|('.join(fileExts)
            exts = r'(' + exts + ')'
            tempName = re.sub(exts,"",temp.name)
            row = df.loc[df[refCol] == tempName]
            temp[key] = map(str,row[key])[0]
            temp = syn.store(temp,forceVersion = False)
        print ""

def updateFormatTypeByFileName(syn,synId,annoKey,annoDict):
    """
    Audit entity file type annotations
    :param syn:            A Synapse object: syn = synapseclient.login()- Must be logged into synapse
    :param synId:          A Synapse ID of Project, Folder, or File OR a list of Synapse Objects
    :param annoKey:        The annotation key for file type. (i.e. "fileType", "fileFormat", or "formatType")
    :param annoDict        A dict where key is the extension of the filename, 
                           value is the corresponding file type value in entity annotations
        
    Example:
    
       updateFormatTypeByFileName(syn,"syn12345","fileType",{".bam":"bam", ".doc":"word", "bw":"bigwig"})
       OR
       updateFormatTypeByFileName(syn,["syn1","syn2"],"fileType",{".bam":"bam", ".doc":"word", "bw":"bigwig"})
       
    """
    if type(synId) is list:
        print "Input is a list of Synapse Objects \n"
        for synID in synId:
            temp = synID
            print "Accessing File %s ..." % temp.id
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
            temp = syn.store(temp,forceVersion = False)
            print "> Done!"
            break
    if tempType == "":
        print "> ERROR: file type not found in file types dictionary"
    print ""
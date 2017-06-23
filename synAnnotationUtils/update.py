import logging
import re
import pandas
import synapseclient
import synapseutils
from synapseclient.entity import is_container


def _helperUpdateAnnoByDict(syn, synEntity, annoDict, forceVersion):
    logging.info("Updating annotations...")
    synEntity.annotations.update(annoDict)
    synEntity = syn.store(synEntity, forceVersion=forceVersion)
    logging.info("Completed.")


## by dict
def updateAnnoByDict(syn, synId, annoDict, forceVersion=False):
    """
    Update annotations by giving a dict
    :param syn:            A Synapse object: syn = synapseclient.login()- Must be logged into synapse
    :param synId:          A Synapse ID of Project, Folder, or File OR a list of Synpase Objects
    :param annoDict        A dict of annotations
    :param forceVersion    Default is False
    
    Example:

       updateAnnoByDict(syn,"syn12345",{"dataType":"testing","projectName":"foo"})
       OR
       updateAnnoByDict(syn,["syn1","syn2"],{"dataType":"testing","projectName":"foo"})
       
    """

    if type(synId) is list:  # Output from audit functions
        logging.info("Input is a list of Synapse Objects")
        for synEntity in synId:
            logging.info("Accessing File %s ..." % synEntity.id)
            _helperUpdateAnnoByDict(syn, synEntity, annoDict)
    else:
        logging.info("Input is a Synpase ID")
        synEntity = syn.get(synId, downloadFile=False)
        if not is_container(synEntity):
            logging.info("%s is a File" % synId)
            _helperUpdateAnnoByDict(syn, synEntity, annoDict, forceVersion)
        else:
            directory = synapseutils.walk(syn, synId)
            for dirpath, dirname, filename in directory:
                for i in filename:
                    synEntity = syn.get(i[1], downloadFile=False)
                    logging.info("Getting File %s ..." % i[1])
                    _helperUpdateAnnoByDict(syn, synEntity, annoDict, forceVersion)


## by idDict
def updateAnnoByIdDictFromDict(syn, idDict, annoDict, forceVersion=False):
    """
    Update annotations from dictionary
    by a given dict(key:annotation keys need to updated, value:a list of Synpase Objects)
    
    :param syn:            A Synapse object: syn = synapseclient.login()- Must be logged into synapse
    :param idDict:         A dict.Key is the annotations key, value is a list of Synapse Objects
    :param annoDict        A dict of annotations
    :param forceVersion    Default is False

    Example:

        updateAnnoByIdDictFromDict(syn,{"dataType":["syn1","syn2"]},{"dataType":"csv","test":"TRUE"})
       
    """

    for key in idDict:
        logging.info("Updating annotaion values for key: %s" % key)
        for synEntity in idDict[key]:
            logging.info(synEntity.id)
            synEntity[key] = annoDict[key]
            synEntity = syn.store(synEntity, forceVersion=forceVersion)
        logging.info("")


def _helperUpdateAnnoByMetadata(syn, synEntity, metaDf, refCol, cols2Add, fileExts, forceVersion):
    """
    This helper function is built for PsychENCODE project data release.
    The entity name without extension is map to a column in the metadata data frame

    """

    logging.info("Updating annotations by metadata...")

    exts = ')|('.join(fileExts)
    exts = r'(' + exts + ')'
    synEntityName = re.sub(exts, "", synEntity.name)

    row = metaDf.loc[metaDf[refCol] == synEntityName]
    if row.empty:
        logging.warning("missing metadata")
    else:
        for colName in cols2Add:
            logging.info("%s " % colName)
            synEntity[colName] = map(str, row[colName])[0]
        synEntity = syn.store(synEntity, forceVersion=forceVersion)
    logging.info("")


def updateAnnoByMetadata(syn, synId, metaDf, refCol, cols2Add, fileExts, forceVersion=False):
    """
    Audit entity annotations against metadata
    :param syn:            A Synapse object: syn = synapseclient.login()- Must be logged into synapse
    :param synId:          A Synapse ID of Project, Folder, or File OR a list of Synpase Objects
    :param metaDf          A pandas data frame of entity metadata
    :param refCol          A name of the column in metaDf that matching one of the entity attributes
    :param cols2Add        A list of columns in metaDf need to be added as entity annotations 
    :param fileExts        A list of all file extensions (PsychENCODE ONLY!!!) 
    :param forceVersion    Default is False

    Example:
    
        updateAnnoByMetadata(syn,"syn12345",metadata,"id",["dataType","tester"],[".bam",".csv"])
       
    """

    if type(synId) is list:  # Output from audit functions
        logging.info("Input is a list of Synapse Objects")
        for synEntity in synId:
            logging.info("Accessing File %s ..." % synEntity.id)
            _helperUpdateAnnoByMetadata(syn, synEntity, metaDf, refCol, cols2Add, fileExts, forceVersion)
    else:
        logging.info("Input is a Synpase ID")
        synEntity = syn.get(synId, downloadFile=False)
        if not is_container(synEntity):
            logging.info("%s is a File" % synId)
            _helperUpdateAnnoByMetadata(syn, synEntity, metaDf, refCol, cols2Add, fileExts, forceVersion)
        else:
            directory = synapseutils.walk(syn, synId)
            for dirpath, dirname, filename in directory:
                for i in filename:
                    synEntity = syn.get(i[1], downloadFile=False)
                    logging.info("Getting File %s ..." % i[1])
                    _helperUpdateAnnoByMetadata(syn, synEntity, metaDf, refCol, cols2Add, fileExts, forceVersion)


def updateAnnoByIdDictFromMeta(syn, idDict, metaDf, refCol, fileExts, forceVersion=False):
    """
    Update annotations from metadata 
    by a given dict(key:annotation keys need to updated, value:a list of Synpase Objects)
    
    :param syn:            A Synapse object: syn = synapseclient.login()- Must be logged into synapse
    :param idDict:         A dict.Key is the annotations key, value is a list of Synapse IDs
    :param metaDf          A pandas data frame of entity metadata
    :param refCol          A name of the column in metaDf that matching one of the entity attributes
    :param fileExts        A list of all file extensions (PsychENCODE ONLY!!!) 
    :param forceVersion    Default is False

    This function is built for PsychENCODE project data release. 
    The entity name without extension is map to a column in the metadata data frame
       
    """

    for key in idDict:
        logging.info("updating annotaion values for key: %s" % key)
        for synEntity in idDict[key]:
            logging.info(synEntity.id)
            exts = ')|('.join(fileExts)
            exts = r'(' + exts + ')'
            synEntityName = re.sub(exts, "", synEntity.name)
            row = df.loc[df[refCol] == synEntityName]
            synEntity[key] = map(str, row[key])[0]
            synEntity = syn.store(synEntity, forceVersion=forceVersion)
        logging.info("")


def _helperUpdateFormatTypeByFileName(syn, synEntity, annoKey, annoDict, forceVersion):
    logging.info("Updating %s..." % annoKey)
    synEntityName = synEntity.name
    synEntityType = ""
    for ext in annoDict.keys():
        if synEntityName.endswith(ext):
            synEntityType = annoDict[ext]
            synEntity[annoKey] = synEntityType
            synEntity = syn.store(synEntity, forceVersion=forceVersion)
            logging.info("Done!")
            break

    if synEntityType == "":
        logging.warning("ERROR: File type not found in file types dictionary")
    logging.info("")


def updateFormatTypeByFileName(syn, synId, annoKey, annoDict, forceVersion=False):
    """
    Audit entity file type annotations
    :param syn:            A Synapse object: syn = synapseclient.login()- Must be logged into synapse
    :param synId:          A Synapse ID of Project, Folder, or File OR a list of Synapse Objects
    :param annoKey:        The annotation key for file type. (i.e. "fileType", "fileFormat", or "formatType")
    :param annoDict        A dict where key is the extension of the filename, 
                           value is the corresponding file type value in entity annotations
    :param forceVersion    Default is False
        
    Example:
    
       updateFormatTypeByFileName(syn,"syn12345","fileType",{".bam":"bam", ".doc":"word", "bw":"bigwig"})
       OR
       updateFormatTypeByFileName(syn,["syn1","syn2"],"fileType",{".bam":"bam", ".doc":"word", "bw":"bigwig"})
       
    """
    if type(synId) is list:  # Output from audit functions
        logging.info("Input is a list of Synapse Objects")
        for synEntity in synId:
            logging.info("Accessing File %s ..." % synEntity.id)
            _helperUpdateFormatTypeByFileName(syn, synEntity, annoKey, annoDict, forceVersion)
    else:
        logging.info("Input is a Synpase ID")
        synEntity = syn.get(synId, downloadFile=False)
        if not is_container(synEntity):
            logging.info("%s is a File" % synId)
            _helperUpdateFormatTypeByFileName(syn, synEntity, annoKey, annoDict, forceVersion)
        else:
            directory = synapseutils.walk(syn, synId)
            for dirpath, dirname, filename in directory:
                for i in filename:
                    synEntity = syn.get(i[1], downloadFile=False)
                    logging.info("Getting File %s ..." % i[1])
                    _helperUpdateFormatTypeByFileName(syn, synEntity, annoKey, annoDict, forceVersion)


def _csv2df(path):
    """
    Reads users .csv file containing updated annotations and converts it into a pandas dataframe while removing all NA rows
    :param path: Current working directory absolute/relative path to user-defined manifest .csv file containing updated cells with the same schema as existing entity-view
    :return:
    """

    df = pandas.read_csv(path).dropna(how='all')

    return df


def _query2df(syn, synId, clause):
    """
    Downloads the entity-view query result as a CsvFileTable then convert it into a pandas dataframe with ROW_IDs as indecies
    :param syn:         A Synapse object: syn = synapseclient.login(username, password) - Must be logged into synapse
    :param synId:       A Synapse ID of an entity-view (Note: Edit permission on its' files is required)
    :param clause:      A SQL clause to allow for sub-setting & row filtering in order to reduce the data-size on dow
    :return:            A dataframe with the entity-view schema and content
    """
    query = 'select * from '

    if clause is None:
        view = syn.tableQuery(query + synId)
    else:
        view = syn.tableQuery(query + synId + ' ' + clause)

    df = view.asDataFrame()

    return df


def updateEntityView(syn, synId, path, clause=None):
    """
     Update Entity-View annotations by giving a user-defined manifest csv path with the same schema as the Entity-View
      :param syn:            A Synapse object: syn = synapseclient.login(username, password) - Must be logged into synapse
      :param synId:          A Synapse ID of an entity-view (Note: Edit permission on its' files is required)
      :param path:           Current working directory absolute/relative path to user-defined manifest .csv file containing updated cells with the same schema as existing entity-view
      :param clause:         A SQL clause to allow for sub-setting & row filtering in order to reduce the data-size on download

      Example:
         
         updateEntityView(syn, 'syn12345', 'myproject_annotation_updates.csv')
         OR
         updateEntityView(syn, 'syn12345', 'myproject_annotation_updates.csv', 'where assay = 'geneExpression')
      """

    user_df = _csv2df(path)
    view_df = _query2df(syn, synId, clause)

    if user_df.empty:
        logging.info("Uploaded dataframe is empty with nothing to update!")
    else:
        view_df.update(user_df)

    schema_match = list(user_df.columns[~user_df.columns.isin(view_df.columns)])

    if not schema_match:
        logging.info("Updated annotation's dataframe and entity-view's %s schemas don't match." % synId)
    else:
        logging.info("Updated annotations on entity-view %s." % synId)
        view = syn.store(synapseclient.Table(synId, view_df))

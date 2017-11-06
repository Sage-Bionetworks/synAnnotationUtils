import re
import six
import csv
import pandas
import logging
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
            row = metaDf.loc[metaDf[refCol] == synEntityName]
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


def _makeIndex(df):
    """
    Synapse entity-view rows unique-id or index is defined based on three values: ROW_ID, ROW_VERSION, and etag.
    This function concatenates ROW_ID, ROW_VERSION, and etag column values by '_' and assigns it as the data frame
    input index. Hint: the input data frame should not be generated by Synapse utility functions such as .asDataFrame()

    :param df:  A data frame containing synapse minimal schema
    :return:    A data frame containing synapse minimal schema with row index defined as ROW_ID, ROW_VERSION,
                and etag column values concatenated by '_'.
    """
    unique_id = pandas.Series(['ROW_ID', 'ROW_VERSION'])

    if not unique_id.isin(df.columns).all():
        logging.info('To update a file view the columns ROW_ID, ROW_VERSION, and etag must exist.')
    else:
        index = df.apply(lambda x: '%s_%s' % (x['ROW_ID'], x['ROW_VERSION']), axis=1)
        df.insert(0, 'index', index)
        df.set_index(index, inplace=True, drop=True)
        df.drop(['index'], axis=1, inplace=True)

        return df


def _csv2df(path):
    """
    Reads users .csv file containing updated annotations and converts it into a pandas data frame
    while removing rows and columns with all NA values.

    :param path:      Current working directory absolute/relative path to user-defined manifest .csv file containing
                      updated cells with the same schema as existing entity-view
    :return:          A data frame containing modified information on an entity-view structured as.
                      synapse file-ids by standard minimal synapse columns + annotation's schema
    """

    df = pandas.read_csv(path).dropna(how='all')
    df = df.dropna(axis=1, how='all')
    df = _makeIndex(df)

    return df


def _getView(syn, view_id, clause=None):
    """
    Based on a user-defined query calls to synapse's tableQuery function and returns the entity-view generator object.

    :param syn:      A Synapse object: syn = synapseclient.login(username, password) - Must be logged into synapse
    :param view_id:  A Synapse ID of an entity-view (Note: Edit permission on its' files is required)
    :param clause:   A SQL clause to allow for sub-setting & row filtering in order to reduce the data-size
    :return:         An object of type synapse entity-view
    """

    if clause is None:
        query = "".join(['select * from ', view_id])
        view = syn.tableQuery(query)
    else:
        query = "".join(['select * from ', view_id, ' ', clause])
        view = syn.tableQuery(query)

    return view


def query2df(syn, view_id, clause):
    """
    Converts an entity-view query result into a pandas data frame, creates an index column by concatenating ROW_IDs,
    ROW_VERSION, and etags with an underscore ('_') symbol, and sets the index column as the data frames' index then
    drops the index column.

    :param syn:         A Synapse object: syn = synapseclient.login(username, password) - Must be logged into synapse
    :param view_id:     A Synapse ID of an entity-view (Note: Edit permission on its' files is required)
    :param clause:      A SQL clause to allow for sub-setting & row filtering in order to reduce the data-size
    :return:            A data frame containing synapse minimal schema with row index defined as ROW_ID, ROW_VERSION,
                        and etag column values concatenated by '_'.
    """

    view = _getView(syn=syn, view_id=view_id, clause=clause)

    f = csv.DictReader(file(view.filepath))
    df = pandas.DataFrame(list(f))
    df = _makeIndex(df)

    return df


def _dropSynapseIndices(df):
    """
    Removes synapse schema class 'ROW_VERSION' and 'ROW_ID' columns.

    :param df:  An entity-view data frame with the possible 'ROW_VERSION' and 'ROW_ID' columns
    :return:    An entity-view data frame without the 'ROW_VERSION' and 'ROW_ID' columns
    """
    if {'ROW_VERSION', 'ROW_ID'}.issubset(df.columns):
        df.drop(['ROW_VERSION', 'ROW_ID'], axis=1, inplace=True)
    return df


def _checkSave(syn, new_view, current_view, schema_id):
    """
    Checks if the user defined schema of updated entity-view matches the current entity-view.
    If and only if the schemas matches, then it stores the updated entity-view by passing the schema id
    and entity-view data frame. If not, then it informs the user that the update was not completed and
    lists the unmatched columns in schema.

    :param new_view:      An entity-view data frame with updated cells
    :param current_view:  An existing entity-view data frame with updated cells
    :param schema_id:     The schemas' synapse id of the existing entity-view
    :return:              None, with an updated entity-view on synapse or a logged info on which columns
                          caused the schema mismatch if no errors occurs.
    """

    new_view = _dropSynapseIndices(new_view)

    matching_columns = new_view.columns.isin(current_view.columns)
    schema_unmatch = list(new_view.columns[~matching_columns])

    if not schema_unmatch:
        logging.info("Updating annotations on entity-view %s." % schema_id)
        view = syn.store(synapseclient.Table(schema_id, new_view))
    else:
        schema_unmatch_names = ''.join(map(str, schema_unmatch))
        logging.info(
            "Updated data frame and entity-view's %s schema names %s don't match." % (schema_id, schema_unmatch_names))


def updateEntityView(syn, syn_id, path, clause=None):
    """
    Updates Entity-View annotations by giving a user-defined csv path with the same schema as the Entity-View.

    required columns in csv file: ROW_ID, ROW_VERSION, etag
    limitations: This function does not track possible different states (i.e. changes from other users) of an
    entity-view between the time an entity-view has been downloaded and modified until the time it will be uploaded.

    :param syn:       A Synapse object: syn = synapseclient.login(username, password) - Must be logged into synapse
    :param syn_id:    A Synapse ID of an entity-view (Note: Edit permission on its' files is required)
    :param path:      Current working directory absolute/relative path to user-defined manifest .csv file containing
                      updated cells with the same schema as the existing entity-view.
                      df.apply(lambda x: '%s_%s' % (x['ROW_ID'], x['ROW_VERSION']), axis=1)
    :param clause:    A SQL clause to allow for sub-setting & row filtering in order to reduce the data-size on
                      download

    Examples:

             updateEntityView(syn, 'syn12345', 'myproject_annotation_updates.csv')
             updateEntityView(syn, 'syn12345', 'myproject_annotation_updates.csv',
                             'where assay = 'geneExpression')
    """
    if not isinstance(path, six.string_types) and not ".csv" in path:
        raise ValueError("The provided path: %s is not a string or a .csv file path" % path)

    else:
        user_df = _csv2df(path)
        current_view = query2df(syn, syn_id, clause)

        if user_df.empty:
            logging.info("Uploaded data frame is empty with nothing to update!")

        else:
            view_df = current_view
            view_df.update(user_df)

            _checkSave(syn=syn, new_view=view_df, current_view=current_view, schema_id=syn_id)

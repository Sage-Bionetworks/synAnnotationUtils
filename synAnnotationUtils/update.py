import re
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


def _csv2df(path, index_col):
    """
    Reads users .csv file containing updated annotations and converts it into a pandas data frame
    while removing rows and columns with all NA values.

    :param path:      Current working directory absolute/relative path to user-defined manifest .csv file containing
                      updated cells with the same schema as existing entity-view
    :param index_col:  Column or field name of the synapse ROW_IDs and Row_Versions and etag concatenated by _.

    :return:          A data frame containing modified information on an entity-view.
                      Structured as synapse file Ids X standard minimal synapse schema + annotation's schema
    """

    df = pandas.read_csv(path, index_col=index_col).dropna(how='all')
    df = df.dropna(axis=1, how='all')

    return df


def _getView(syn, view_id, clause=None):
    """

    :param syn:
    :param view_id:
    :param clause:
    :return:
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
    ROW_VERSION, and etags with an underscore ('_') symbol, and sets the index column as the data frames' index without
    dropping the index column.

    :param syn:         A Synapse object: syn = synapseclient.login(username, password) - Must be logged into synapse
    :param view_id:       A Synapse ID of an entity-view (Note: Edit permission on its' files is required)
    :param clause:      A SQL clause to allow for sub-setting & row filtering in order to reduce the data-size on dow
    :return:            A dataframe with the entity-view schema and content
    """

    view = _getView(syn=syn, view_id=view_id, clause=clause)

    f = csv.DictReader(file(view.filepath))
    df = pandas.DataFrame(list(f))

    index = df.apply(lambda x: '%s_%s_%s' % (x['ROW_ID'], x['ROW_VERSION'], x['etag']), axis=1)
    df.insert(0, 'index', index)
    df.set_index(index, inplace=True, drop=False)

    return df


def dropSynapseIndices(view_df):
    """
    Removes synapse schema class 'ROW_VERSION' and 'ROW_ID' columns.

    :param view_df:  An entity-view data frame with the possible 'ROW_VERSION' and 'ROW_ID' columns
    :return:         An entity-view data frame without the 'ROW_VERSION' and 'ROW_ID' columns
    """
    if {'ROW_VERSION', 'ROW_ID'}.issubset(view_df.columns):
        view_df.drop(['ROW_VERSION', 'ROW_ID'], axis=1, inplace=True)
    return view_df


def dropIndices(index_col, view_df):
    """
    Removes user specified index column in addition to synapse schema class 'ROW_VERSION' and 'ROW_ID' columns.

    :param index_col:  A user specified index column name
    :param view_df:    An entity-view data frame with the possible user specified index column, 'ROW_VERSION',
                       and 'ROW_ID' columns
    :return:           An entity-view data frame without the user specified index column, 'ROW_VERSION',
                       and 'ROW_ID' columns.
    """

    if {index_col, 'ROW_VERSION', 'ROW_ID'}.issubset(view_df.columns):
        view_df.drop([index_col, 'ROW_VERSION', 'ROW_ID'], axis=1, inplace=True)
    return view_df


def _checkSave(syn, new_view, current_view, schema_id, index_col='index'):
    """
    Checks if the schema of updated entity-view matches the current entity-view. If and only if the schemas match
    then it stores the updated entity-view by passing the schema id and entity-view data frame. If not, then it
    informs the user that the update was not completed and lists the unmatched columns in schema.

    :param new_view:      An entity-view data frame with updated cells
    :param current_view:  An existing entity-view data frame with its current cells as it apears on synapse
    :param schema_id:     The schemas' synapse id of the existing entity-view

    :return:              None, with an updated entity-view on synapse or a logged info on which columns
                          caused the schema mismatch if no errors occurs.
    """

    schema_unmatch = list(new_view.columns[~new_view.columns.isin(current_view.columns)])

    if not schema_unmatch:
        logging.info("Updated annotations on entity-view %s." % schema_id)

        current_view = dropIndices(index_col, current_view)

        view = syn.store(synapseclient.Table(schema_id, current_view))
    else:
        schema_unmatch_names = ''.join(map(str, schema_unmatch))
        logging.info(''.join(["Updated annotation's data frame and entity-view's ", schema_id, " schema names ",
                              schema_unmatch_names, " don't match."]))


def updateEntityView(syn, syn_id, path, index_col='index', clause=None):
    """
    Update Entity-View annotations by giving a user-defined manifest csv path with the same schema as the Entity-View

    :param syn:       A Synapse object: syn = synapseclient.login(username, password) - Must be logged into synapse
    :param syn_id:     A Synapse ID of an entity-view (Note: Edit permission on its' files is required)
    :param path:      Current working directory absolute/relative path to user-defined manifest .csv file containing
                      updated cells with the same schema as existing entity-view
    :param index_col
    :param clause:    A SQL clause to allow for sub-setting & row filtering in order to reduce the data-size on
                      download

    Examples:

             updateEntityView(syn, 'syn12345', 'myproject_annotation_updates.csv', 'index')
             updateEntityView(syn, 'syn12345', 'myproject_annotation_updates.csv', 'index',
                             'where assay = 'geneExpression')
    """

    user_df = _csv2df(path, index_col)
    view_df = query2df(syn, syn_id, clause)

    if user_df.empty:
        logging.info("Uploaded data frame is empty with nothing to update!")
    else:
        view_df.update(user_df)

    _checkSave(syn=syn, new_view=user_df, current_view=view_df, schema_id=syn_id, index_col=index_col)


def expandFields(syn, project_id, view_id, scopes, view_name, path, index_col='index', view_type='file', clause=None,
                 delta=False):
    """
    Based on an existing entity-view, create a new entity-view with additional fields and/or columns.
    user may choose to keep or discard the initial entity-view since this function would duplicate an entity-view.
    Both existing and new columns must be present and not empty in user's csv file.

    :param syn:         A Synapse object: syn = synapseclient.login(username, password) - Must be logged into synapse
    :param project_id:   A Synapse ID of a project (Note: Edit permission is required)
    :param view_id:      A Synapse ID of an entity-view (Note: Edit permission on its' files is required)
    :param scopes:      A list of Projects/Folders or their ids
    :param view_name:    The name of your entity-view
    :param path:        Current working directory absolute/relative path to user-defined manifest .csv file containing
                        updated fields and cells based on and in addition to an existing entity-view schema.
    :param index_col
    :param view_type:    The type of entity-view to display: either 'file' or 'project'. Defaults to 'file'
    :param clause:      A SQL clause to allow for sub-setting & row filtering in order to reduce the data-size on
                        download
    :param delta:       Boolean indicating whether to delete the entity-view with the original schema. Default is False

    Example:

        expandFields(syn, 'syn1234', 'syn3333', ['syn1255', 'syn1266'], 'my projects entityview',
                    'myproject_annotation_updates.csv', 'index', viewType='file', clause=None, delta=True)
    """

    user_df = _csv2df(path, index_col)
    user_df = dropIndices(index_col, user_df)

    minimal_view_schema_column_names = [x['name'] for x in syn.restGET("/column/tableview/defaults/file")['list']]

    # Find existing and new fields/columns in users entity-views schema
    new_cols = list(user_df.columns[~user_df.columns.isin(minimal_view_schema_column_names)].dropna())

    # TODO: pull the existing columns types
    col_types = pandas.DataFrame(user_df[new_cols].dtypes)
    col_types = col_types.replace(['object', 'int64', 'float64'], ['STRING', 'INTEGER', 'FLOAT'])

    added_cols = [syn.store(synapseclient.Column(name=k, columnType=col_types.loc[k, 0])) for k in new_cols]
    added_cols_ids = [c['id'] for c in added_cols]

    logging.info("Creating a new entity-view schema based on %s." % view_id)

    schema = syn.store(synapseclient.EntityViewSchema(name=view_name, columns=added_cols_ids, parent=project_id,
                                                      scopes=scopes, view_type=view_type))

    new_view = query2df(syn=syn, view_id=schema.id, clause=clause)

    _checkSave(syn=syn, new_view=user_df, current_view=new_view, schema_id=schema.id, index_col=index_col)

    if delta:
        logging.info(''.join(['Deleting previous entity-view: ', view_id]))
        previous_view = syn.get(view_id)
        syn.delete(previous_view)


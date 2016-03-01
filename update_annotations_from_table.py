"""This notebook updates all file annotations a project.

There are four required file annotations for any assay file uploaded to Synapse.

`UID` - a universal ID for the sample represented in the file
`dataType`: `mRNA`, `miRNA`, `methylation`, etc.
`fileType`: `bam`, `fastq`, etc.
`fileSubType`: is `fileType`-specific; e.g., `bam` can have `mapped` or `unmapped`.

This script uses the `dataType` field to get assay-specific files (using a query lookup dictionary `assayToQuery`),
and then the `UID` to merge existing file annotations and the table metadata (which is obtained through a lookup
dictionary `assayToMetadataTable`.

"""

import sys
import pandas as pd
import numpy
import logging

logger = logging.getLogger('Annotation Updater')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)

from multiprocessing.dummy import Pool
mp = Pool(10)

import synapseclient

syn = synapseclient.Synapse(skip_checks=True)
syn.login(silent=True)

# Taken from https://github.com/larssono/Analysis_helpers/blob/master/synapseHelpers.py
# Should probably be in synapseclient.synapseutils
SYNAPSE_PROPERTIES = ['benefactorId', 'nodeType', 'concreteType', 'createdByPrincipalId',
                      'createdOn', 'createdByPrincipalId', 'eTag', 'id', 'modifiedOn',
                      'modifiedByPrincipalId', 'noteType', 'versionLabel', 'versionComment',
                      'versionNumber', 'parentId', 'description', 's3Token', 'name']

def query2df(queryContent, filterSynapseFields=True, savedSynapseFields = ('id', 'name')):
    """Converts the returned query object from Synapse into a Pandas DataFrame.

    Arguments:
    - `queryContent`: content returned from query or chunkedQuery
    - `filterSynapseFields`: Removes Synapse properties of the entity returned with "select * ..."
                             defaults to True
    - `savedSynapseFields`: list of synapse fields not to filter defaults to id, name

    """

    removedProperties = set(SYNAPSE_PROPERTIES) - set(savedSynapseFields)

    try:
        queryContent = queryContent['results']
    except TypeError: #If 'results' not present it is a geneartor
        queryContent = list(queryContent)
    for row in queryContent:
        for key in row.keys():
            prefix, new_key = key.split('.', 1)
            item = row.pop(key)
            row[new_key] = item[0] if type(item) is list else item
            if filterSynapseFields and new_key in removedProperties:
                del row[new_key]
    return pd.DataFrame(queryContent)

def fixDict(d):
    """Fix dictionaries having .

    """

    newd = {}

    for k, v in d.iteritems():
        tmp = {}

        for k2, v2 in v.iteritems():

            if v2 is list:
                tmp[k2] = v2
            else:
                try:
                    if not numpy.isnan(v2):
                        tmp[k2] = v2
                except TypeError:
                    tmp[k2] = v2

        newd[k] = tmp

    return newd

def updateDict(a, b, removeFields=SYNAPSE_PROPERTIES):
    """Remove synapse fields from Synapse query on file annotations and update with a second dictionary.

    """

    foo = [a.pop(x) for x in removeFields]
    a.update(b)
    return a

def updateAnnots(synid, lookupDict, syn):
    """Update annotations using a dict to lookup which annotations to add (keys are synids).

    Works this way for parallelization.

    """

    logger.info("Updating annotations for %s" % synid)

    o = syn.get(synid, downloadFile=False)
    a = syn.getAnnotations(o)

    # Make a copy so updates do notaffect the original
    newa = a.copy()

    newa = updateDict(newa, lookupDict[o.properties.id])

    # setAnnotations does a syn.store, so not explicitly needed
    foo = syn.setAnnotations(o, newa)

def main():

    import yaml
    import argparse
    import pprint
    import pandas

    parser = argparse.ArgumentParser("Update annotations on Synapse files from Synapse table-based metadata.")
    parser.add_argument("-c", "--config", help="YAML config file (requires dataType list, dataTypesToMetadataTable dict, and dataTypesToQuery dict).",
                        ype=str)
    parser.add_argument("--dry-run", help="Perform the requested command without updating anything in Synapse.",
                        action="store_true", default=False)


    args = parser.parse_args()

    with file(args.config) as f:
        config = yaml.load(f)

    logger.info(config)
    dataTypes = config['dataTypes']
    dataTypesToMetadataTable = config['dataTypesToMetadataTable']
    dataTypesToQuery = config['dataTypesToQuery']

    for dataType in dataTypes:

        # Metadata
        logger.info("Getting %s metadata" % dataType)
        metaSchema = syn.get(dataTypesToMetadataTable[dataType])
        metaResults = syn.tableQuery("select * from %s" % metaSchema.id)
        meta = metaResults.asDataFrame()

        # All files
        logger.info("Getting %s file list" % dataType)
        fileTbl = query2df(syn.chunkedQuery(dataTypesToQuery[dataType]))

        # Merge metadata and files
        logger.info("Merging %s" % dataType)
        merged = pd.merge(left=fileTbl[['id', 'UID']], right=meta,
                          how="left", left_on="UID", right_on="UID")

        # Set a new index and drop it as a column
        merged.index = merged.id
        merged.drop("id", axis=1, inplace=True)

        # Transpose the data and convert it to a dictionary, fix individual entries
        mergedDict = merged.transpose().to_dict()
        mergedDict2 = fixDict(mergedDict)

        # Update the annotations
        if not args.dry_run:
            logger.info("Updating %s annotations" % dataType)
            res = mp.map(lambda x: updateAnnots(x, mergedDict2), mergedDict2.keys())
        else:
            logger.info("Would have updated:")
            merged.to_csv(sys.stdout, sep="\t")

if __name__ == "__main__":
    main()

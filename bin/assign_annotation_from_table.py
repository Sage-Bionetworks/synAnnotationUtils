import sys

import synapseclient
syn = synapseclient.login()
import pandas as pd

def combineRows(group):
    if len(group) > 1:
        group = group.to_dict(orient='list')
        for k,v in group.iteritems():
            valSet = set(v)
            group[k] = ','.join(str(val) for val in valSet)
        return pd.DataFrame(group,index=[0])
    else:
        return group


df = syn.tableQuery('SELECT * FROM %s' % sys.argv[1])
df = df.asDataFrame()

annotationDf = df.groupby('synapseId').apply(combineRows)
annotationDf.set_index(['synapseId'],inplace=True)
annotationDict = annotationDf.to_dict(orient='index')

for key,value in annotationDict.iteritems():
	ent = syn.get(key,downloadFile=False)
	ent.annotations = value
	ent = syn.store(ent,forceVersion=False)
	print(ent.id)
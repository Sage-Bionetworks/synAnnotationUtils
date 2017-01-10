import yaml
import synapseutils as synu



def checkAgainstDict(syn, synId, annotDictId,verbose=True):	
	"""
	Compares annotations in use against dictionary.
	Gets all annotation keys and values in use in a project and compares against those specified by a dictionary. Prints non-matching terms.

	:param syn:            A Synapse object: syn = synapseclient.login()- Must be logged into synapse
	:param synId:          A Synapse ID of Project or Folder
	:param annotDictId     A Synapse ID of annotation dictionary in YAML

	Return:
		If synId is an ID of a Project/Folder
			wrongKeys:		    A set of keys in use in synId that are not in annotDictId.
			wrongValues: 	    A set of values in use in synId that are not in annotDictId.

	Example:
		wrongKeys, wrongValues = checkAgainstDict(syn,"syn12345","syn45678")

	"""

	yamlEnt = syn.get(annotDictId)
	with open(yamlEnt.path) as f:
		annotations = yaml.load(f)

	allKeysInProject = set()
	allValsInProject = set()

	systemKeysToExclude = ['creationDate', 'etag', 'id', 'uri', 'accessControl']

	directory = synu.walk(syn,synId)
	for dirpath,dirname,files in directory:
		for item in files:
			temp = syn.getAnnotations(item[1])
			for key in temp:
				if key in systemKeysToExclude: continue
				allKeysInProject.add(key)
				if isinstance(temp[key], list):
					for val in temp[key]:
						allValsInProject.add(str(val))
				else:
					allValsInProject.add(str(temp[key]))

	print 'Number of key terms in project: %d' % len(allKeysInProject)
	print 'Number of value terms in project: %d' % len(allValsInProject)
	wrongKeys = set()
	wrongValues = set()

	allKeysInVocab = set(annotations.keys())
	if verbose and not allKeysInProject <= allKeysInVocab:
		print '\nKeys in use that are not found in dictionary: \n'
		wrongKeys = allKeysInProject.difference(allKeysInVocab)
		for item in wrongKeys:
			print '%s' % item

	allValsInVocab = set()
	for key, vl in annotations.iteritems():
		if isinstance(vl, list):
			for element in vl:
				allValsInVocab.add(str(element))
		else:	
			allValsInVocab.add(str(vl))
	if verbose and not allValsInProject <= allValsInVocab:
		print '\nValues in use that are not found in dictionary: \n'
		wrongValues = allValsInProject.difference(allValsInVocab)
		for item in wrongValues:
			print '%s' % item
	
	return(wrongKeys, wrongValues)



def countQueryResults(sql,syn):
	'''Counts number of entities returned by a query.'''

	results = syn.chunkedQuery(sql)
	count = 0
	for i in results:
		count += 1
	return count


def countPerAnnot(annotDictSynID, project, syn, grouping=None):
	'''Counts instances of key-value pairs.

	Counts number of items annotated to each key-value pair in the given dictionary within the given project.'''

	yamlEnt = syn.get(annotDictSynID)
	with open(yamlEnt.path) as f:
	    annotations = yaml.load(f)

	if grouping is not None: # This block counts annotation terms stratified by one term as specified in argument 'grouping'

		for element in annotations[grouping]:
			print '%s' % element
			for key in annotations:
				print '%s' % key
				if isinstance(annotations[key], list):
					for val in annotations[key]:
						sql = 'select id from file where projectId=="%s" and file.%s=="%s" and file.%s=="%s"' % (project, key, val, grouping, element)
						print '%s: %s\t%d' % (key, val, countQueryResults(sql, syn))
				else:
					sql = 'select id from file where projectId=="%s" and file.%s=="%s" and file.%s=="%s"' % (project, key, annotations[key], grouping, element)
					print '%s: %s\t%d' % (key, annotations[key], countQueryResults(sql, syn))

	else: # This block counts annotation terms across the whole project

		for key in annotations:
			print '%s' % key
			if isinstance(annotations[key], list):
				for val in annotations[key]:
					sql = 'select id from file where projectId=="%s" and file.%s=="%s"' % (project, key, val)
					print '%s: %s\t%d' % (key, val, countQueryResults(sql, syn))
			else:
				sql = 'select id from file where projectId=="%s" and file.%s=="%s"' % (project, key, annotations[key])
				print '%s: %s\t%d' % (key, annotations[key], countQueryResults(sql, syn))



def updateKey(oldKey,newKey,inAnnot):
	'''Replaces oldKey with newKey.
	
	Given a dictionary, replaces oldKey with newKey, keeping any existing value assigned to that term.'''

	if oldKey in inAnnot:
		inAnnot[newKey] = inAnnot[oldKey]
		del inAnnot[oldKey]
	return(inAnnot)



def correctAnnot(syn,synId,projSynId,correctionsFile):
	"""
	Propagates annotation changes based on tab-delimited input.
	Given a tab-separated file containing annotations to be updated, changes annotations across a project. File contains one line per key-value pair, if line has two entries, they are assumed to be oldKey and newKey, if three entries, they are assumed to be key, oldValue, newValue.'''

	:param syn:            A Synapse object: syn = synapseclient.login()- Must be logged into synapse
	:param synId:          A Synapse ID of Project or Folder
	:param projSynId:      A Synapse ID of Project (possibly duplicate of synId).
	:param correctionsFile Path to a tab-delimited file of old and new annotation values.

	Example:
		correctAnnot(syn,"syn12345","syn45678","annotation_corrections.txt")

	"""

	with open(correctionsFile) as toChange:
		for line in toChange:
			items = line.strip().split('\t')
			if len(items) == 2: # update keys
				old = items[0]
				new = items[1]
				directory = synu.walk(syn,synId)
				for dirpath,dirname,files in directory:
					for item in files:
						temp = syn.getAnnotations(item[1])
						if old not in temp: continue
						correctedAnnotations = updateKey(oldKey=old,newKey=new,inAnnot=temp)
						savedAnnotations = syn.setAnnotations(result['file.id'],correctedAnnotations)
			elif len(items) > 2: # update values
				kKey = items.pop(0)
				old = items.pop(0)
				sql = 'select id,%s from file where projectId=="%s" and file.%s=="%s"' % (kKey,projSynId,kKey,old)
				results = syn.chunkedQuery(sql)
				for result in results:
					temp = syn.getAnnotations(result['file.id'])
					if kKey not in temp: continue
					temp[kKey] = items
					savedAnnotations = syn.setAnnotations(result['file.id'],temp)


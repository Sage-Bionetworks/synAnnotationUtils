import yaml


def checkAgainstDict(annotDict, project, syn){
	''' Compares annotations in use in a project against those specified by a dictionary and prints the ones that do not match the dictionary.'''
	yamlEnt = syn.get(annotDict)
	annotations = yaml.load(file(yamlEnt.path))

	allKeysInProject = set()
	allValsInProject = set()

	systemKeysToExclude = ['creationDate', 'etag', 'id', 'uri', 'accessControl']

	results = syn.chunkedQuery('select id from file where projectId=="%s"' % project)
	for result in results:
		temp = syn.getAnnotations(result['file.id'])
		for key in temp:
			if key in systemKeysToExclude: continue
			if not key in allKeysInProject:
				allKeysInProject.add(key)
			if isinstance(temp[key], list):
				for val in temp[key]:
					if str(val) in allValsInProject: continue
					allValsInProject.add(str(val))
			else:
				if str(temp[key]) in allValsInProject: continue
				allValsInProject.add(str(temp[key]))

	print 'Number of key terms in project: %d' % len(allKeysInProject)
	print 'Number of value terms in project: %d' % len(allValsInProject)


	allKeysInVocab = set(annotations.keys())
	if not allKeysInProject <= allKeysInVocab:
		print 'Keys in use that are not found in dictionary: '
		for item in allKeysInProject.difference(allKeysInVocab):
			print '%s' % item


	allValsInVocab = list()
	for key in annotations:
		temp = annotations[key]
		if isinstance(temp, list):
			for element in temp:
				allValsInVocab.append(str(element))
		else:	
			allValsInVocab.append(str(temp))
	allUniqueValsInVocab = set(allValsInVocab)
	if not allValsInProject <= allValsInVocab:
		print 'Values in use that are not found in dictionary: '
		for item in allValsInProject.difference(allValsInVocab):
			print '%s' % item
 
}


def countQueryResults(sql,syn):
	'''Counts number of entities returned by a query.'''

	results = syn.chunkedQuery(sql)
	count = 0
	for i in results:
		count += 1
	return count


def countPerAnnot(annotDict, project, syn, grouping=None){
	'''Counts number of items annotated to each key-value pair in the given dictionary within the given project.'''

	yamlEnt = syn.get(annotDict)
	annotations = yaml.load(file(yamlEnt.path))

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
}


def updateKey(oldKey,newKey,inAnnot):
	'''Given a dictionary, replaces oldKey with newKey, keeping any existing value assigned to that term.'''

	if oldKey in inAnnot:
		inAnnot[newKey] = inAnnot[oldKey]
		del inAnnot[oldKey]
	return(inAnnot)



def correctAnnot(correctionsFile, project,syn){
	'''Given a tab-separated file containing annotations to be updated, changes annotations across a project. File contains one line per key-value pair, if line has two entries, they are assumed to be oldKey and newKey, if three entries, they are assumed to be key, oldValue, newValue.'''

	with open(correctionsFile) as toChange:
		for line in toChange:
			items = line.strip().split('\t')
			if len(items) == 2: # update keys
				old = items[0]
				new = items[1]
				sql = 'select id,%s from file where projectId=="%s"' % (old,project)
				results = syn.chunkedQuery(sql)
				for result in results:
					temp = syn.getAnnotations(result['file.id'])
					if old not in temp: continue
					correctedAnnotations = updateKey(oldKey=old,newKey=new,inAnnot=temp)
					savedAnnotations = syn.setAnnotations(result['file.id'],correctedAnnotations)
			elif len(items) > 2: # update values
				kKey = items.pop(0)
				old = items.pop(0)
				sql = 'select id,%s from file where projectId=="%s" and file.%s=="%s"' % (kKey,project,kKey,old)
				print sql
				results = syn.chunkedQuery(sql)
				for result in results:
					temp = syn.getAnnotations(result['file.id'])
					if kKey not in temp: continue
					temp[kKey] = items
					savedAnnotations = syn.setAnnotations(result['file.id'],temp)
}
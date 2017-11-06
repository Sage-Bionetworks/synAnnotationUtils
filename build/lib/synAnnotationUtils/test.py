import pandas
import synapseclient
import synAnnotationUtils
from synAnnotationUtils import update
from nose.tools import assert_equals, assert_less

syn = synapseclient.login()

viewId = 'syn10142572'
view = syn.tableQuery('select * from %s' % viewId)
view_df = view.asDataFrame()

view_df.iloc[1, view_df.columns.get_loc('name')] = 'mappingcombo_copy2.txt'
view_df.iloc[2, view_df.columns.get_loc('name')] = 'mappingcombo_copy.txt'

view_df.to_csv("changed_view.csv")
synAnnotationUtils.update.updateEntityView(syn, 'syn10142572', '/Users/nasim/Documents/sage-internship/sage-projects/synAnnotationUtils/changed_view.csv')

view = syn.tableQuery('select * from %s' % viewId)
view_df = view.asDataFrame()
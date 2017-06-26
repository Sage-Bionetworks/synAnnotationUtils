import pandas
import synapseclient
import synAnnotationUtils
from synAnnotationUtils import update
from nose.tools import assert_equals, assert_less


def test_update_entityview():
    '''
    TODO:    Make test denovo
    :return: None
    '''
    syn = synapseclient.login()

    viewId = 'syn10142572'
    fileName = 'changed_view.csv'

    view = syn.tableQuery('select * from %s' % viewId)
    view_df = view.asDataFrame()

    # make some changes and save the changes to a csv file
    view_df.iloc[1, view_df.columns.get_loc('medRecord')] = 'test'
    view_df.to_csv(fileName)

    # update the entity-view
    synAnnotationUtils.update.updateEntityView(syn=syn, synId=viewId, path=fileName, indexCol='Unnamed: 0')

    # Get the updated entity-view and convert into dataframe
    view = syn.tableQuery('select * from %s' % viewId)
    view_df = view.asDataFrame()

    # test if changes have been made to annotations
    assert_equals(view_df.iloc[1, view_df.columns.get_loc('medRecord')], 'test')

    # revert changes back to original
    view_df.iloc[1:3, view_df.columns.get_loc('medRecord')] = 'mapping'
    view = syn.store(synapseclient.Table(viewId, view_df))





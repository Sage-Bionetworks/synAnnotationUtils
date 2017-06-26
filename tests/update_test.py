import pandas
import synapseclient
import synAnnotationUtils
from synAnnotationUtils import update
from nose.tools import assert_equals


def test_update_entityview():
    '''
    TODO:    Make test denovo
    :return: None

        Example: nosetests -vs tests/update_test.py:test_update_entityview
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


def test_copy_and_update_entityview():
    '''
    TODO:    Make test denovo
    :return: None

         Example: nosetests -vs tests/update_test.py:test_copy_and_update_entityview
    '''
    syn = synapseclient.login()

    parentId = 'syn8392128'
    viewId = 'syn10142572'
    scopeList = ['syn8392160']
    fileName = 'changed_view.csv'
    newName = 'adding columns test'
    colIndex = 'Unnamed: 0'

    view = syn.tableQuery('select * from %s' % viewId)
    view_df = view.asDataFrame()

    # add a column to the entity-view dataframe and save as csv
    col_df = pandas.DataFrame({'labResult': ['pos', 'neg', 'neg', 'neg']})
    col_df.set_index(view_df.index, inplace=True)
    view_df = view_df.join(col_df)
    view_df.to_csv(fileName)

    synAnnotationUtils.update.expandFields(syn, parentId, viewId, scopeList, newName, fileName, colIndex, viewType='file', clause=None, delta=False)



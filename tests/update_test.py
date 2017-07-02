import pandas
import synapseclient
import synAnnotationUtils
from synAnnotationUtils import update
from nose.tools import assert_equals


def test_update_entityview():
    '''
    :return: None or Error

        Example: nosetests -vs tests/update_test.py:test_update_entityview
    '''
    syn = synapseclient.login()

    view_id = 'syn10142572'
    file_name = 'changed_view.csv'

    # Get the current entity-view data frame from synapse
    view_df = synAnnotationUtils.update.query2df(syn, view_id, clause=None)

    # make some changes to its' annotations and save the changed entity-view data frame to a csv file
    view_df.iloc[1, view_df.columns.get_loc('medRecord')] = 'test'

    # Users may choose to save the data frame as csv files in which case the data frame index must not be saved
    # view_df.to_csv(file_name, index=False)

    # update the entity-view annotations on synapse using the csv file
    synAnnotationUtils.update.updateEntityView(syn=syn, syn_id=view_id, path=file_name, index_col='index')

    # Get the updated entity-view data frame from synapse
    view_df = synAnnotationUtils.update.query2df(syn, view_id, clause=None)

    # test if changes have been made to annotations
    assert_equals(view_df.iloc[1, view_df.columns.get_loc('medRecord')], 'test')

    # revert changes back to original
    view_df.iloc[1:3, view_df.columns.get_loc('medRecord')] = 'mapping'

    view_df = synAnnotationUtils.update.dropIndices(index_col='index', view_df=view_df)

    view = syn.store(synapseclient.Table(view_id, view_df))


def test_copy_and_update_entityview():
    '''
    :return: None or Error

         Example: nosetests -vs tests/update_test.py:test_copy_and_update_entityview
    '''
    syn = synapseclient.login()

    parent_id = 'syn8392128'
    view_id = 'syn10142572'
    scope_list = ['syn8392160']
    file_name = 'changed_view.csv'
    new_name = 'adding columns test 100'
    col_index = 'index'

    view_df = synAnnotationUtils.update.query2df(syn, view_id, clause=None)

    # add a non-empty column to the entity-view data frame and save it as a csv file
    col_df = pandas.DataFrame({'labResult': ['pos', 'neg', 'neg', 'neg']})
    col_df.set_index(view_df.index, inplace=True)
    view_df = view_df.join(col_df)

    view_df = synAnnotationUtils.update.dropSynapseIndices(view_df)

    # Users may choose to save the data frame as csv files in which case the data frame index must not be saved
    # view_df.to_csv(file_name, index=False)

    synAnnotationUtils.update.expandFields(syn, parent_id, view_id, scope_list, new_name, file_name, col_index,
                                           view_type='file', clause=None, delta=False)



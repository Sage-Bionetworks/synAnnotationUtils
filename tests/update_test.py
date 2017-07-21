import os
import pandas
import synapseclient
import synAnnotationUtils
from synAnnotationUtils import update
from nose.tools import assert_equals


def test_update_entityview():
    """
    Annotations test project on synapse: https://www.synapse.org/#!Synapse:syn10163670

    :return: None or Error

        Example: nosetests -vs tests/update_test.py:test_update_entityview
    """
    syn = synapseclient.login()

    view_id = 'syn10168977'
    file_name = 'changed_view_test.csv'

    # Get the current entity-view data frame from synapse
    view_df = synAnnotationUtils.update.query2df(syn, view_id, clause=None)

    # make some changes to its' annotations and save the changed entity-view data frame to a csv file
    view_df.iloc[1, view_df.columns.get_loc('center')] = 'TCGA'

    # Users may choose to save the data frame as csv files in which case the data frame index must not be saved
    view_df.to_csv(file_name, index=False)

    # update the entity-view annotations on synapse using the csv file
    synAnnotationUtils.update.updateEntityView(syn=syn, syn_id=view_id, path=file_name, clause=None)

    # Get the updated entity-view data frame from synapse
    updated_df = synAnnotationUtils.update.query2df(syn, view_id, clause=None)

    # test if changes have been made to annotations
    assert_equals(updated_df.iloc[1, updated_df.columns.get_loc('center')], 'TCGA')

    # revert changes back to original
    updated_df.iloc[1:3, updated_df.columns.get_loc('center')] = 'labA'

    # store reverted entity-view
    view = syn.store(synapseclient.Table(view_id, updated_df))

    # delete created file
    os.remove("changed_view_test.csv")


def test_copy_and_update_entityview():
    """
    Annotations test project on synapse: https://www.synapse.org/#!Synapse:syn10163670

    :return: None or Error

         Example: nosetests -vs tests/update_test.py:test_copy_and_update_entityview
    """
    syn = synapseclient.login()

    parent_id = 'syn10163670'
    view_id = 'syn10168977'
    scope_list = ['syn10163672']
    file_name = 'changed_view_test.csv'
    new_name = 'adding columns test july 21 2017 - v2'

    view_df = synAnnotationUtils.update.query2df(syn, view_id, clause=None)

    # add a non-empty column to the entity-view data frame and save it as a csv file
    col_df = pandas.DataFrame({'labResult': ['pos', 'neg', 'neg', 'neg']})
    col_df.set_index(view_df.index, inplace=True)

    view_df = view_df.join(col_df)
    view_df.to_csv(file_name, index=False)

    # copy another entity-view based on view_id and expand columns to it
    synAnnotationUtils.update.expandFields(syn, parent_id, view_id, scope_list, new_name, file_name,
                                           view_type='file', clause=None, delta=False)

    # delete created file
    os.remove("changed_view_test.csv")


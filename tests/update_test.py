import os
import pandas
import synapseclient
import synAnnotationUtils
from synAnnotationUtils import update
from nose.tools import assert_equals


def test_update_entityview():
    """
    Applies simple modifications to annotations of entity-view syn10168977 located at
    Annotations test project on synapse: https://www.synapse.org/#!Synapse:syn10163670
    and reverts back the changes applied.

    :return: None or Error
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

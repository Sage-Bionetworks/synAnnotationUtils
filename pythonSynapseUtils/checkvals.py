""" Check that two or more .csv files stored on Synapse
    have the same values given key(s) to align them.

Output is written to inconsistent_vals.csv.

Author: Phil Snyder (July, 2016)
"""

import argparse
import pandas as pd
import synapseclient

def read_args():
    """ Read in arguments from the command line.
    Args:
        (None)
    Returns:
        ref (str): Synapse ID of .csv file to use as reference.
        ids (list): Synapse IDs of .csv files to compare.
        key (str or list): key(s) to align files upon.
        key_map (str): a key mapping based on Synapse ID.
    Raises:
        TypeError: --ids or --key is left empty.
    """
    parser = argparse.ArgumentParser(description="Check that two \
            .csv files have the same values given a key.")
    parser.add_argument('ref', help="Synapse ID of .csv file \
            to use as reference")
    parser.add_argument('--ids', metavar='synId', nargs="+",
            help="Synapse IDs of .csv files to compare")
    parser.add_argument('--key', help="Key(s) to align upon (post key-mapping)")
    parser.add_argument('--key-map', help="path to .json file \
            containing an array of synapse IDs as objects, each of \
            which maps to another json object containing key:value pairs \
            specifying how to rename columns (see source code for example)")
    '''
        To rename 'Individual ID' to 'Individual_Id' and 'Age (Years)'
        to 'Age' in syn1234 and 'Gender' to 'Sex' in syn5678 your .json
        file should look like this:
            [
                {"syn1234":
                    {
                    "Individual ID":"Individual_Id",
                    "Age (Years)":"Age"
                    }
                },
                {"syn5678":
                    {
                    "Gender":"Sex"
                    }
                }
            ]
    '''
    args = parser.parse_args()
    if not len(args.ids) or not len(args.key):
        raise TypeError("Must supply both a key and at least one Synapse ID \
                to check against the reference .csv file.")
    return args.ref, args.ids, args.key, args.key_map

def get_data(ref, ids):
    """ Download .csv files by their Synapse ID.
    Args:
        ids (list): list of Synapse IDs
    Returns:
        data (list): list of synapseclient.entity.File
    Raises:
        SynapseHTTPError: if there is an id in ids that does not exist.
    """
    syn = synapseclient.Synapse()
    print("Logging into Synapse...")
    syn.login()
    print("Downloading .csv files...")
    data = [syn.get(i) for i in ids + [ref]]
    return data

def rename_cols(data, key_map):
    """ Rename columns in .csv files
    Usually you would use this function to match the column names of
    the .csv files from --ids with the column names of ref (see read_args()).
    Args:
        data (list): list of synapseclient.entity.File.
        key_map(str): json representation of column mapping.
    Returns:
        dataframes (dict): mapping of Synapse ID to pandas dataframe.
        file_name_map (dict): mapping of Synapse IDs to file names.
    Raises:
        IndexError: if there is a mapping from 'a' to 'b' in key_map
            but 'a' does not exist in the original column names.
    """
    key_map = pd.read_json(key_map)
    id_map = {d['id']:d for d in data}
    file_name_map = {d['id']:d.name for d in data}
    dataframes = {d.id : pd.read_csv(d.path, header=0, dtype=str) for d in data}
    for i in key_map:
        d = id_map[i]
        df = pd.read_csv(d.path, header=0, dtype=str)
        mapping = key_map[i][0]
        for m in mapping:
            if not m in df.columns:
                raise IndexError("'" + m + "' is not a column name in " + i)
        df = df.rename(columns=mapping)
        dataframes[i] = df
    return dataframes, file_name_map

def check_for_incorrect_vals(ref, key, dataframes, file_name_map, key_map):
    """ Compare values in shared columns to reference.
    Args:
        ref (str): Synapse ID of reference .csv.
        key (str): key column to match upon.
        dataframes (dict): mapping of Synapse ID to pandas dataframe.
        file_name_map (dict): mapping of Synapse IDs to file names.
        key_map (str): a key mapping based on Synapse ID.
    Returns:
        (None) Writes output to 'inconsistent_vals.csv'.
    Raises:
        (None)
    """
    reference = dataframes[ref]
    key_map = pd.read_json(key_map)
    wrong = {'file_name':[], 'synId':[], 'colInFile':[],
            'colInOriginal':[], 'actualValue':[], 'correctValue':[]}
    for synId in dataframes:
        if synId in key_map:
            mappings = key_map[synId][0]
            reversed_mappings = {mappings[k] : k for k in mappings}
        merged = pd.merge(reference, dataframes[synId], on=key, how='inner')
        shared_cols = set(reference.columns).intersection(\
                dataframes[synId].columns)
        shared_cols.remove(key)
        for col in shared_cols:
            bool_match = (merged[col+'_x'] == merged[col+'_y'])
            if all(bool_match):
                continue
            else:
                for i in range(len(bool_match)):
                    if not bool_match[i] and not (pd.isnull(merged[col+'_x'][i])
                                and pd.isnull(merged[col+'_y'][i])):
                        wrong['file_name'].append(file_name_map[synId])
                        wrong['synId'].append(synId)
                        if col in reversed_mappings:
                            wrong['colInFile'].append(reversed_mappings[col])
                        else:
                            wrong['colInFile'].append(col)
                        wrong['colInOriginal'].append(col)
                        wrong['actualValue'].append(merged[col+'_y'][i])
                        wrong['correctValue'].append(merged[col+'_x'][i])
    wrong_df = pd.DataFrame(wrong, index = range(len(wrong['file_name'])))
    wrong_df.to_csv("inconsistent_vals.csv", index=False)
    if not len(wrong_df['file_name']):
        print("No inconsistent values!")
    else:
        print("Inconsistent values found. Results stored in 'inconsistent_vals.csv'.")

def main():
    ref, ids, key, key_map = read_args()
    data = get_data(ref, ids)
    if key_map:
        dataframes, file_name_map = rename_cols(data, key_map)
    else:
        dataframes = {d.id: pd.read_csv(d.path, header=0, dtype=str) for d in data}
        file_name_map = {d.id : d.name for d in data}
    check_for_incorrect_vals(ref, key, dataframes, file_name_map, key_map)

if __name__ == "__main__":
    main()

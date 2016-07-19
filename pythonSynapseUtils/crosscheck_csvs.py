''' crosscheck_csvs.py - find missing values in csv key columns

 There are 2.5 ways to use this script.

 The first way entails manually passing in the Synapse IDs
 and keys to match on using flags in the command line arguments.

 1.
 $ python crosscheck_csvs.py --ids syn1234 syn5678 syn9012 --keys key1 key2

 The second way entails passing newline delimited files containing
 the synapse IDs and keys to examine.

 2.
 $ python crosscheck_csvs.py --file-ids ids.txt --file-keys keys.txt

 You may also combine flags with overlapping functionality
 depending on your needs.

 2.5
 $ python crosscheck_csvs.py --file-ids ids.txt --file-keys keys.txt \
        --keys extraKey

 If there are missing values, the script will output something
 that looks like this:

 b.csv (syn7064702) is missing value '4' in key column a
 a.csv (syn7064118) is missing value 'banana' in key column a
 b.csv (syn7064702) is missing value '13' in key column c
 a.csv (syn7064118) is missing value '@#$!@@#' in key column c
 a.csv (syn7064118) is missing value 'orange' in key column b

 This output will also be written to the file 'missing_values.txt'
 in the current directory (if there are indeed missing values).

 Author: Phil Snyder (July 2016)
'''

from __future__ import print_function
import synapseclient
import pandas as pd
import argparse

def read_command_args():
    '''Read in arguments from the command line.
    Args:
        (See script header)
    Returns:
        ids (set): a list of Synapse IDs of .csv files to search.
        keys (set): a list of key values to search.
    Raises:
        TypeError: if id or key arguments are omitted.
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument("--ids", metavar="synId", nargs="+",
            help="Synapse IDs of csv files")
    parser.add_argument("--keys", metavar="key", nargs="+",
            help="column names to match upon.")
    parser.add_argument("--file-ids", metavar="path",
        help="path to newline delimited file containing \
                Synapse IDs of csv files")
    parser.add_argument("--file-keys", metavar="path",
        help="path to newline delimited file containing \
                column names to match upon.")
    args = parser.parse_args()
    if not (args.ids or args.file_ids):
        raise TypeError("crosscheck_csvs.py requires the Synapse IDs of \
                the .csv files to be checked as input.")
    if not (args.keys or args.file_keys):
        raise TypeError("crosscheck_csvs.py requires the specification of \
                column key names to be checked as input.")
    ids = set()
    keys = set()
    if args.ids:
        for i in args.ids:
            ids.add(i)
    if args.file_ids:
        with open(args.file_ids, 'r') as f:
            new_ids = f.read().strip().split("\n")
            for i in new_ids:
                ids.add(i)
    if args.keys:
        for k in args.keys:
            keys.add(k)
    if args.file_keys:
        with open(args.file_keys, 'r') as f:
            new_keys = f.read().strip().split("\n")
            for k in new_keys:
                keys.add(k)
    return ids, keys

def index_files(ids, keys):
    '''Index the files specified in ids.
    Args:
        ids (set): a list of synapse IDs.
        keys (set): a list of keys to match on.
    Returns:
        checklist (dict): keeps track of which csvs have which key values
            {
                key1: {
                    key1_item1: [synId1, ...],
                    key1_item2: [synId1, ...], ...
                }, ...
            }.
        data (synapseclient.entity.File): information on files
            specified in ids.
    Raises:
        KeyError: if there is a k in keys which does not exist
            in one of the id in ids.
    '''
    print("Logging into Synapse...")
    syn = synapseclient.Synapse()
    syn.login()
    print("Downloading data...")
    data = [syn.get(i) for i in ids]
    print("Indexing data...")
    checklist = {k: {} for k in keys}
    for d in data:
        df = pd.read_csv(d.path, header=0, dtype=str)
        for k in keys:
            for i in df[k]:
                if i in checklist[k]:
                    checklist[k][i].append(d.id)
                else:
                    checklist[k][i] = [d.id]
    return checklist, data

def find_missing_vals(ids, checklist, data):
    '''Search for missing values in the specified key columns.
    Args:
        ids (set): a list of synapse IDs.
        checklist (dict): keeps track of which csvs have which key values
        {
            key1: {
                key1_item1: [synId1, ...],
                key1_item2: [synId1, ...], ...
            }, ...
        }.
        data (synapseclient.entity.File): information on files
            specified in ids.
    Returns:
        (None)
    Raises:
        (None)
    '''
    missing_vals = []
    print("Searching for missing values...")
    for k in checklist:
        for i in checklist[k]:
            for d in data:
                if not d.id in checklist[k][i]:
                    missing_vals.append(
                        {
                        'synId': d.id,
                        'file_name': d.name,
                        'val': i,
                        'key': k
                        })
    if not len(missing_vals):
        print("No missing values!")
    else:
        with open("missing_values.txt", 'w') as f:
            for m in missing_vals:
                sentence = " ".join([m['file_name'],
                    "(" + m['synId'] + ")",
                    "is missing value",
                    "'" + m['val'] + "'",
                    "in key column",
                    m['key']])
                f.write(sentence + "\n")
                print(sentence)

def main():
    ids, keys = read_command_args()
    checklist, data = index_files(ids, keys)
    find_missing_vals(ids, checklist, data)

if __name__ == "__main__":
    main()

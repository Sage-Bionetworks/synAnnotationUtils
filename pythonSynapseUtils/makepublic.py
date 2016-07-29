""" Make private Files public by changing their local permissions
    or moving them to a public folder.

Author: Phil Snyder (July, 2016)
"""
import synapseclient
from synapseclient.__main__ import move
import argparse

def read_command_args():
    """ Read in command line arguments.
    Args:
        (None)
    Returns:
        ids (list): a list of Syanpse IDs to make public.
        new_parentId (str): Synapse ID of new (public) Folder to place ids in.
    Raises:
        (None)
    """
    parser = argparse.ArgumentParser(description="Make files on Synapse \
            viewable to the public.")
    parser.add_argument("--ids", metavar="synID", nargs="+",
            help = "Synapse IDs of files to make public")
    parser.add_argument("--file", help="newline delimited file containing \
            Synapse IDs of Files to make public", type=str)
    parser.add_argument("--new-parentid", type=str,
            help="(public) file to move --ids to")
    args = parser.parse_args()
    if args.file:
        with open(args.file, 'r') as f:
            if args.ids:
                args.ids += f.read().strip().split("\n")
            else:
                args.ids = f.read().strip().split("\n")
    return args.ids, args.new_parentid

def make_public(ids, parentId):
    """ Make Files specified in ids public, possibly by
        moving them to parentId.
    Args:
        ids (list): a list of Syanpse IDs to make public.
        new_parentId (str): Synapse ID of new (public) Folder to place ids in.
    Returns:
        (None)
    Raises:
        TypeError: if ids is empty.
        SynapseHTTPError: if there is an i in ids that does not exist or
            parentId does not exist, or you don't have permission to do
            the things you are trying to do.
    """
    syn = synapseclient.Synapse()
    print("Logging into Synapse...")
    syn.login()
    if parentId:
        print("Moving Files to public Folder...")
        for i in ids:
            args = _synObj(i, parentId)
            move(args, syn)
    else:
        print("Making public...")
        for i in ids:
            syn.setPermissions(i, accessType=['READ'], warn_if_inherits=False)
    print("Done making Files public.")

class _synObj:
    """ Purely for the `move` function when making files public """
    def __init__(self, synId, parentId):
        self.id = synId
        self.parentid = parentId

def main():
    ids, parentId = read_command_args()
    make_public(ids, parentId)

if __name__ == "__main__":
    main()

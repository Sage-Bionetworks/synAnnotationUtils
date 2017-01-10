""" Change permissions on a set of Files.

Author: Phil Snyder (August, 2016)
"""
import synapseclient
from synapseclient.__main__ import move
import argparse

def read_command_args():
    """ Read in command line arguments.
    Args:
        (None)
    Returns:
        ids (list): a list of Syanpse IDs to change permissions of.
        add (list): permissions to add.
        remove (list): permissions to remove.
    Raises:
        (None)
    """
    parser = argparse.ArgumentParser(description="Batch change permissions of \
            Files on Synapse.")
    parser.add_argument("--ids", metavar="synID", nargs="+",
            help = "Synapse IDs of files to modify permissions")
    parser.add_argument("--file", help="newline delimited file containing \
            Synapse IDs of Files to modify permissions")
    parser.add_argument("--add", help="permissions to add")
    parser.add_argument("--remove", help="permissions to remove")
    args = parser.parse_args()
    if args.file:
        with open(args.file, 'r') as f:
            if args.ids:
                args.ids += f.read().strip().split("\n")
            else:
                args.ids = f.read().strip().split("\n")
    listify = lambda x: [x] if type(x) == str else x
    return map(listify, [args.ids, args.add, args.remove])

def modify_permissions(ids, add, remove):
    """ Modify permissions of all Files specified in ids.
    Args:
        ids (list): a list of Syanpse IDs to modify permissions of.
        add (list): permissions to add.
        remove (list): permissions to remove.
    Returns:
        (None)
    Raises:
        TypeError: if ids is empty.
        SynapseHTTPError: if there is an i in ids that does not exist,
            or you don't have permission to modify the permissions of
            an i in ids or you are adding a nonvalid permission type.
    """
    syn = synapseclient.Synapse()
    print("Logging into Synapse...")
    syn.login()
    print("Modifying permissions...")
    for i in ids:
        preexisting_permissions = list([p for p in \
                syn.getPermissions(i, principalId="PUBLIC") \
                if not (remove and p in remove)])
        if add:
            permissions = add + preexisting_permissions
        else:
            permissions = preexisting_permissions
        syn.setPermissions(i, accessType=permissions, principalId="PUBLIC",
                warn_if_inherits=False)
    print("Done changing Files permissions.")

def main():
    ids, add, remove = read_command_args()
    modify_permissions(ids, add, remove)

if __name__ == "__main__":
    main()

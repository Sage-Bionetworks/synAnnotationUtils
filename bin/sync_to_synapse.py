'''Upload files using the Synapse sync manifest.

This is a temporary helper script.
The Synapse client release 1.7.3 will replace this script.

'''

import synapseclient
import synapseutils

def sync(args, syn):
    """Run syncToSynapse using command line arguments.

    """

    synapseutils.syncToSynapse(syn, manifest_file=args.manifestFile,
                               dry_run=args.dryRun, sendMessages=args.sendMessages,
                               retries=args.retries)

def main():
    import argparse

    syn = synapseclient.login()

    parser_sync = argparse.ArgumentParser(description='Synchronize files described in a manifest to Synapse')
    parser_sync.add_argument('--dryRun', action='store_true', default=False,
                             help='Perform validation without uploading.')
    parser_sync.add_argument('--sendMessages', action='store_true', default=False,
                             help='Send notifications via Synapse messaging (email) at specific intervals, on errors and on completion.')
    parser_sync.add_argument('--retries', metavar='INT', type=int, default=4)
    parser_sync.add_argument('manifestFile', metavar='FILE', type=str,
                             help='A tsv file with file locations and metadata to be pushed to Synapse.')

    args = parser_sync.parse_args()
    sync(args, syn)

if __name__ == "__main__":
    main()

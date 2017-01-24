"""Output a Synapse TableSchema to a text file.

JSON or CSV formats are available.

"""

_header = ('id', 'name', 'columnType', 'maximumSize', 'defaultValue', 'enumValues')

def main():
    import argparse
    import csv
    import sys
    import json

    import synapseclient

    parser = argparse.ArgumentParser(description="Output a Synapse TableSchema to a text file.")
    parser.add_argument('id', type=str, help='Synapse ID of a TableSchema')
    parser.add_argument('--format', type=str, choices=['csv', 'json'],
                      default='json', help="Output format [default: %(default)s]")
    args = parser.parse_args()

    syn = synapseclient.login(silent=True)
    d = syn.getTableColumns(args.id)

    if args.format == 'json':
        print json.dumps(list(d), indent=2)
    elif args.format == 'csv':
        wr = csv.DictWriter(sys.stdout, fieldnames=_header)
        wr.writeheader()

        for row in d:
            if row['columnType'] == "STRING":
                try:
                    row['enumValues'] = map(str, row['enumValues'])
                except KeyError:
                    pass

            wr.writerow(row)

if __name__ == "__main__":
  main()

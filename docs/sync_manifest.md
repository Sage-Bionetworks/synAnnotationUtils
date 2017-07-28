## Create sync manifest
1. You can create a sync manifest by simply running 
   ```   
   $ python sync_manifest.py -d home_dir_of_files/ --id syn1234567
   ```
   `syn1234567` is the Synapse ID of a Synapse Project or a Synapse Folder that is the equivalent to `home_dir_of_files` in the directory hierarchy. 

   This will generate a tsv file lists out all the files under `home_dir_of_files` and create mirrored folder hierarchy on Synapse server at the same time. Please note no files have been uploaded at this time. 

   It outputs a tab delimited format in your console.

   | path                             | parent     |
   | -------------------------------- | ---------- |
   | home/home_dir_of_files/file1.txt | syn1234567 |
   | home/home_dir_of_files/file2.txt | syn1234567 |

   If you would like to save and edit the file, you can do 
   ```
   $ python sync_manifest.py -d home_dir_of_files/ --id syn1233 > your_manifest_file.tsv
   ```

2. Add annotation keys
   If you would like to add a set of annotation keys to the files, you can run
   ```
   $ python sync_manifest.py -d home_dir_of_files/ --id syn1234567 -f project_annotations.json > your_manifest_file.tsv
   ```
   We suggest you use the set of Sage standard annotation keys from our [repo](https://raw.githubusercontent.com/Sage-Bionetworks/synapseAnnotations/master/synapseAnnotations/data/common/minimal_Sage_standard.json)

   In this case, the script is 
   ```
   $ python sync_manifest.py -d home_dir_of_files/ --id syn1234567 -f https://raw.githubusercontent.com/Sage-Bionetworks/synapseAnnotations/master/synapseAnnotations/data/common/minimal_Sage_standard.json > your_manifest_file.tsv
   ```

3. Flatten folder hierarchy
   If you know your folder structure very well and would like to flatten the hierarchy, you can add another parameter '-n'.
   ```
   $ python sync_manifest.py -d home_dir_of_files/ --id syn1234567 -f https://raw.githubusercontent.com/Sage-Bionetworks/synapseAnnotations/master/synapseAnnotations/data/common/minimal_Sage_standard.json -n 2 > your_manifest_file.tsv
   ```
   Once you set `n=2`, the folder with a depth greater than 2 will be flattened. 

   For example, if your folder structure on your local server is like below.
   ```   
   --- home_dir_of_files
          --- rnaSeq
               --- sample1
                    --- file1.txt
                    --- file2.csv
               --- sample2 
          --- snpArray
               --- sample1
               --- sample2
   ```
   Since folder 'sample1' under 'rnaSeq' has a depth of 3, the files inside the folder will be renamed to "sample1_file1.txt" and "sample1_file2.csv" and to be placed under "rnaSeq" folder on the Synapse server.

## Upload and annotate files via sync function
After making all the edits to your manifest, you can use sync_to_synapse.py to upload your files to Synapse. Please note this helper script will be replaced by `$ synapse sync your_manifest_file.tsv` after the next Synapse client release.

   ```
$ python sync_to_synapse.py your_manifest_file.tsv
   ```


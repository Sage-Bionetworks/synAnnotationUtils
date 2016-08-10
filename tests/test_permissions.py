from __future__ import print_function
import unittest, uuid, tempfile
import synapseclient as sc
from pythonSynapseUtils import modify_permissions as mp

syn = sc.Synapse()
syn.login()

class TestModifyPermissions(unittest.TestCase):

    def setUp(self):
        print("Creating private Project...")
        test_project = sc.Project("Test" + uuid.uuid4().hex)
        self.project_id = syn.store(test_project).id
        print("Creating Folder...")
        folder = sc.Folder("folder", parent=self.project_id)
        self.folder_id = syn.store(folder).id
        print("Creating File within Folder...")
        with tempfile.NamedTemporaryFile() as temp:
            temp.write("123testingfolder")
            temp.flush()
            temp_file = sc.File(temp.name, parent=self.folder_id)
            self.folder_fileId = syn.store(temp_file).id
        print("Creating File within Project...")
        with tempfile.NamedTemporaryFile() as temp:
            temp.write("123testingproject")
            temp.flush()
            temp_file = sc.File(temp.name, parent=self.project_id)
            self.project_fileId = syn.store(temp_file).id

    def tearDown(self):
        syn.delete(self.project_id)

    def test_add_remove(self):
        mp.modify_permissions([self.folder_fileId, self.project_fileId], \
                ["DELETE", "UPDATE"], None)
        self.assertEqual(set(syn.getPermissions(self.folder_fileId, \
                principalId="PUBLIC")), set(['DELETE', 'UPDATE']))
        self.assertEqual(set(syn.getPermissions(self.project_fileId, \
                principalId="PUBLIC")), set(['DELETE', 'UPDATE']))
        mp.modify_permissions([self.folder_fileId, self.project_fileId], \
                None, ['UPDATE'])
        self.assertEqual(syn.getPermissions(self.folder_fileId, \
                principalId="PUBLIC"), ['DELETE'])
        self.assertEqual(syn.getPermissions(self.project_fileId, \
                principalId="PUBLIC"), ['DELETE'])
        mp.modify_permissions([self.folder_fileId, self.project_fileId], \
                ['DELETE'], ['UPDATE'])
        self.assertEqual(syn.getPermissions(self.folder_fileId, \
                principalId="PUBLIC"), ['DELETE'])
        self.assertEqual(syn.getPermissions(self.project_fileId, \
                principalId="PUBLIC"), ['DELETE'])

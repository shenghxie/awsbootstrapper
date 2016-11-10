import unittest, subprocess, os
from awsinstancebootstrapper import AWSInstanceBootStrapper

class MockManifest(object):
    
    def __init__(self):
        self.getjobOverride = lambda x: None
        self.GetS3DocumentsOverride = lambda x: None
        self.GetS3KeyPrefixOverride = lambda : None
    def GetJob(self, instanceId):
        return self.getjobOverride(instanceId)
    
    def GetS3Documents(self, filter):
        return self.GetS3DocumentsOverride(filter)

    def GetS3KeyPrefix(self):
        return self.GetS3KeyPrefixOverride()

class MockS3Interface(object):

    def __init__(self):
        self.downloadCompressedOverride = lambda keyNamePrefix, documentName, localDir: None
        self.uploadCompressedOverride = lambda keyNamePrefix, documentName, localDir: None

    def downloadCompressed(self, keyNamePrefix, documentName, localDir):
        return self.downloadCompressedOverride(keyNamePrefix, documentName, localDir)

    def uploadCompressed(self, keyNamePrefix, documentName, localDir):
        return self.uploadCompressedOverride(keyNamePrefix, documentName, localDir)

class MockInstanceManager(object):
    def __init__(self):
        self.uploadInstanceDataMock = lambda instanceId, s3DocumentName, localPath: None

    def uploadInstanceData(self, instanceId, s3DocumentName, localPath):
        return self.uploadInstanceDataMock(instanceId, s3DocumentName, localPath)

class AWSInstanceBootStrapper_Test(unittest.TestCase):

    def test_Constructor(self):
        m = MockManifest()
        i = MockInstanceManager()
        def getJob(instanceId):
            self.assertEqual(instanceId, 1)
            return { "RequiredS3Data": "abcd" }

        m.getjobOverride = getJob
        s = MockS3Interface()
        a = AWSInstanceBootStrapper(1, m, s, i, "logpath")
        self.assertEqual(a.instanceId, 1)
        self.assertEqual(a.manifest, m)
        self.assertEqual(a.s3interface, s)
        self.assertEqual(a.instancemanager, i)
        self.assertEqual(a.logpath, "logpath")
        self.assertEqual(a.job, getJob(1))
        self.assertEqual(a.job["RequiredS3Data"], getJob(1)["RequiredS3Data"])

    def test_UploadLog(self):
        m = MockManifest()
        i = MockInstanceManager()

        def uploadInstanceDataMock(instanceId, s3DocumentName, localPath):
            self.assertEqual(instanceId, 100)
            self.assertEqual(s3DocumentName, "logfilename", os.path.join("logpath","logfilename"))

        i.uploadInstanceDataMock = uploadInstanceDataMock
        def getJob(instanceId):
            self.assertEqual(instanceId, 100)
            return { "RequiredS3Data": "abcd" }

        m.getjobOverride = getJob
        s = MockS3Interface()
        a = AWSInstanceBootStrapper(100, m, s, i, os.path.join("logpath","logfilename"))
        a.UploadLog()

    def test_DownloadS3Documents(self):
        m = MockManifest()

        def getJob(instanceId):
            self.assertEqual(instanceId, 999)
            return { "RequiredS3Data": 
                    ["doc1", "doc2", "doc3" ] }

        def GetS3Documents(filter):
            directions = {
                "doc1": "LocalToAWS", 
                "doc2": "AWSToLocal", 
                "doc3": "Static"
                }
            self.assertTrue(filter["Name"] in ["doc1", "doc2", "doc3" ])
            return [{
                "Name": filter["Name"],
                "Direction": directions[filter["Name"]],
                "LocalPath": "LocalPath",
                "AWSInstancePath": "AWSInstancePath"
            }]

        def GetPrefixOverride():
            return "prefix"

        m.getjobOverride = getJob
        m.GetS3DocumentsOverride = GetS3Documents
        m.GetS3KeyPrefixOverride = GetPrefixOverride

        def downloadCompressed(keyNamePrefix, documentName, localDir):
            self.assertEqual(keyNamePrefix, GetPrefixOverride())
            self.assertTrue(documentName in ["doc1", "doc3"]) # doc1 and doc3 are downloadable
            self.assertNotEqual(documentName, "doc2") # doc 2 is upload from instance only
            self.assertEqual(localDir, "AWSInstancePath")

        s = MockS3Interface()
        s.downloadCompressed = downloadCompressed
        i = MockInstanceManager()
        a = AWSInstanceBootStrapper(999, m, s, i, "logpath")
        a.DownloadS3Documents()

    def test_RunCommands(self):

        def getJob(instanceId):
            self.assertEqual(instanceId, 9999)
            return {
                "RequiredS3Data": ["doc1", "doc2", "doc3" ],
                "Commands": [
                    {
                        "Command": "ls",
                        "Args": ["-l", "-h"]
                    }
                ]
            }

        m = MockManifest()
        m.getjobOverride = getJob
        s = MockS3Interface()
        i = MockInstanceManager()
        def uploadInstanceDataMock(instanceId, s3DocumentName, localPath):
            self.UploadCalled = True
            self.assertEqual(instanceId, 9999)
            self.assertEqual(s3DocumentName, "logfilename", os.path.join("logpath","logfilename"))

        i.uploadInstanceDataMock = uploadInstanceDataMock
        a = AWSInstanceBootStrapper(9999, m, s, i, os.path.join("logpath","logfilename"))
        result = a.RunCommands()
        self.assertTrue(result)
        self.assertTrue(self.UploadCalled)

    def test_RunCommandReturnsFalseAndUploadsLogOnInvalidCommand(self):

        def getJob(instanceId):
            self.assertEqual(instanceId, 77)
            return {
                "RequiredS3Data": ["doc1", "doc2", "doc3" ],
                "Commands": [
                    {
                        "Command": "_an_invalid_command_",
                        "Args": ["-dosomething"]
                    }
                ]
            }

        m = MockManifest()
        m.getjobOverride = getJob
        s = MockS3Interface()
        i = MockInstanceManager()
        def uploadInstanceDataMock(instanceId, s3DocumentName, localPath):
            self.UploadCalled = True
            self.assertEqual(instanceId, 77)
            self.assertEqual(s3DocumentName, "logfilename", os.path.join("logpath","logfilename"))

        i.uploadInstanceDataMock = uploadInstanceDataMock
        a = AWSInstanceBootStrapper(77, m, s, i, os.path.join("logpath","logfilename"))
        self.assertRaises(Exception, a.RunCommands)
        self.assertTrue(self.UploadCalled)
        

    def test_UploadS3Documents(self):
        m = MockManifest()

        def getJob(instanceId):
            self.assertEqual(instanceId, 999)
            return { "RequiredS3Data": 
                    ["doc1", "doc2", "doc3" ] }

        def GetS3Documents(filter):
            directions = {
                "doc1": "LocalToAWS", 
                "doc2": "AWSToLocal", 
                "doc3": "Static"
                }
            self.assertTrue(filter["Name"] in ["doc1", "doc2", "doc3" ])
            return [{
                "Name": filter["Name"],
                "Direction": directions[filter["Name"]],
                "LocalPath": "LocalPath",
                "AWSInstancePath": "AWSInstancePath"
            }]

        def GetPrefixOverride():
            return "prefix"

        m.getjobOverride = getJob
        m.GetS3DocumentsOverride = GetS3Documents
        m.GetS3KeyPrefixOverride = GetPrefixOverride

        def uploadCompressed(keyNamePrefix, documentName, localDir):
            self.assertEqual(keyNamePrefix, GetPrefixOverride())
            self.assertTrue(documentName not in ["doc1", "doc3"]) # doc1 and doc3 are downloadable
            self.assertEqual(documentName, "doc2") # doc 2 is upload from instance only
            self.assertEqual(localDir, "AWSInstancePath")

        s = MockS3Interface()
        s.uploadCompressed = uploadCompressed
        i = MockInstanceManager()
        a = AWSInstanceBootStrapper(999, m, s, i, "logpath")
        a.UploadS3Documents()

if __name__ == '__main__':
    unittest.main()

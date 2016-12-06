import unittest, subprocess, os
from mock import Mock, call
from awsinstancebootstrapper import AWSInstanceBootStrapper
from manifest import Manifest
from instancemanager import InstanceManager
from s3interface import S3Interface
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
        self.uploadInstanceLogMock = lambda instanceId: None
        self.downloadInstanceLogMock = lambda instanceId, localDir: None 

    def uploadInstanceLog(self, instanceId):
        return self.uploadInstanceLogMock(instanceId)

    def downloadInstanceLog(self, instanceId, localDir):
        return self.downloadInstanceLogMock(instanceId, localDir)

    def uploadInstanceData(self, instanceId, s3DocumentName, localPath):
        return self.uploadInstanceDataMock(instanceId, s3DocumentName, localPath)

class AWSInstanceBootStrapper_Test(unittest.TestCase):

    def test_Constructor(self):
        m = Mock(spec=Manifest)
        i = Mock(spec=InstanceManager)

        m.GetJob.return_value = { "RequiredS3Data": "abcd" }
        s = MockS3Interface()
        a = AWSInstanceBootStrapper(1, m, s, i)
        self.assertEqual(a.instanceId, 1)
        self.assertEqual(a.manifest, m)
        self.assertEqual(a.s3interface, s)
        self.assertEqual(a.instancemanager, i)
        self.assertEqual(a.job, { "RequiredS3Data": "abcd" })
        
        m.GetJob.assert_called_once_with(1)

    def test_UploadLog(self):
        m = Mock(spec=Manifest)
        m.GetJob.return_value = { "RequiredS3Data": "a" }
        i = Mock(spec=InstanceManager)
        s = Mock(spec=S3Interface)
        a = AWSInstanceBootStrapper(100, m, s, i)
        a.UploadLog()
        i.uploadInstanceLog.assert_called_once_with(100)
        m.GetJob.assert_called_once_with(100)

    def test_DownloadS3Documents(self):
        m = Mock(spec=Manifest)
        s = Mock(spec=S3Interface)
        i = Mock(spec=InstanceManager)

        m.GetJob.return_value = { "RequiredS3Data": ["doc1", "doc2", "doc3" ] }
        directions = {
            "doc1": "LocalToAWS", 
            "doc2": "AWSToLocal", 
            "doc3": "Static"
        }
        m.GetS3Documents.side_effect = lambda filter: [
            {
                "Name": filter["Name"],
                "Direction": directions[filter["Name"]],
                "LocalPath": "LocalPath",
                "AWSInstancePath": "{0}AWSInstancePath".format(filter["Name"])
            }
        ]
        m.GetS3KeyPrefix.return_value = "prefix"

        a = AWSInstanceBootStrapper(999, m, s, i)
        a.DownloadS3Documents()
        self.assertEqual(s.downloadCompressed.call_count, 2)
        s.downloadCompressed.assert_has_calls([
            call("prefix", "doc1", "{0}AWSInstancePath".format("doc1")),
            call("prefix", "doc3", "{0}AWSInstancePath".format("doc3"))
            ])

        i.uploadInstanceLog.assert_any_call(999)

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
            self.uploadCompressedWasCalled = True
            self.assertEqual(keyNamePrefix, GetPrefixOverride())
            self.assertTrue(documentName not in ["doc1", "doc3"]) # doc1 and doc3 are downloadable
            self.assertEqual(documentName, "doc2") # doc 2 is upload from instance only
            self.assertEqual(localDir, "AWSInstancePath")

        s = MockS3Interface()
        s.uploadCompressed = uploadCompressed
        
        i = MockInstanceManager()
        def uploadInstanceLog(instanceId):
            self.uploadLogCalled = True
            self.assertTrue(instanceId == 999)

        i.uploadInstanceLogMock = uploadInstanceLog
        a = AWSInstanceBootStrapper(999, m, s, i)
        a.UploadS3Documents()
        self.assertTrue(self.uploadCompressedWasCalled)
        self.assertTrue(self.uploadLogCalled)

if __name__ == '__main__':
    unittest.main()

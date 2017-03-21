import unittest, subprocess, os
from mock import Mock, call
from awsinstancebootstrapper import AWSInstanceBootStrapper
from manifest import Manifest
from instancemanager import InstanceManager
from s3interface import S3Interface

class AWSInstanceBootStrapper_Test(unittest.TestCase):

    def test_Constructor(self):
        m = Mock(spec=Manifest)
        i = Mock(spec=InstanceManager)
        s = Mock(spec=S3Interface)
        m.GetJob.return_value = { "RequiredS3Data": "abcd" }
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
        m = Mock(spec=Manifest)
        s = Mock(spec=S3Interface)
        i = Mock(spec=InstanceManager)
        m.GetJob.return_value = {
            "RequiredS3Data": ["doc1", "doc2", "doc3" ],
            "Commands": [
                {
                    "Command": "ls",
                    "Args": ["-l", "-h"]
                }
            ]
        }

        a = AWSInstanceBootStrapper(9999, m, s, i)
        result = a.RunCommands()
        self.assertTrue(result)
        i.uploadInstanceLog.assert_any_call(9999)

    def test_RunCommandReturnsFalseAndUploadsLogOnInvalidCommand(self):
        m = Mock(spec=Manifest)
        s = Mock(spec=S3Interface)
        i = Mock(spec=InstanceManager)

        m.GetJob.return_value = {
                "RequiredS3Data": ["doc1", "doc2", "doc3" ],
                "Commands": [
                    {
                        "Command": "_an_invalid_command_",
                        "Args": ["-dosomething"]
                    }
                ]
            }

        a = AWSInstanceBootStrapper(77, m, s, i)
        self.assertRaises(Exception, a.RunCommands)
        i.uploadInstanceLog.assert_any_call(77)
        

    def test_UploadS3Documents(self):
        m = Mock(spec=Manifest)
        s = Mock(spec=S3Interface)
        i = Mock(spec=InstanceManager)

        m.GetJob.return_value = { "RequiredS3Data": ["doc1", "doc2", "doc3", "doc4" ]}
        directions = {
            "doc1": "LocalToAWS", 
            "doc2": "AWSToLocal", 
            "doc3": "Static",
            "doc4": "AWSToLocal"
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

        #def uploadCompressed(keyNamePrefix, documentName, localDir):
        #    self.uploadCompressedWasCalled = True
        #    self.assertEqual(keyNamePrefix, GetPrefixOverride())
        #    self.assertTrue(documentName not in ["doc1", "doc3"]) # doc1 and doc3 are downloadable
        #    self.assertEqual(documentName, "doc2") # doc 2 is upload from instance only
        #    self.assertEqual(localDir, "AWSInstancePath")

        #s = MockS3Interface()
        #s.uploadCompressed = uploadCompressed

        #i = MockInstanceManager()
        #def uploadInstanceLog(instanceId):
        #    self.uploadLogCalled = True
        #    self.assertTrue(instanceId == 999)

        #i.uploadInstanceLogMock = uploadInstanceLog
        a = AWSInstanceBootStrapper(101, m, s, i)
        a.UploadS3Documents()
        s.uploadCompressed.assert_has_calls([
            call("prefix", "doc2", "{0}AWSInstancePath".format("doc2")),
            call("prefix", "doc4", "{0}AWSInstancePath".format("doc4"))
            ])
        i.uploadInstanceLog.assert_any_call(101)
        #self.assertTrue(self.uploadCompressedWasCalled)
        #self.assertTrue(self.uploadLogCalled)

if __name__ == '__main__':
    unittest.main()

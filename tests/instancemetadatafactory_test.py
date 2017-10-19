from mock import Mock
import unittest, os, json
from timehelper import TimeHelper
from manifest import Manifest
from instancemetadatafactory import InstanceMetadataFactory
from instancemetadata import InstanceMetadata

class InstanceMetadataFactory_Test(unittest.TestCase):

    def setUp(self):
        self.path = os.path.join(os.getcwd(),"tests","test_metadata.json")

    def tearDown(self):
        if os.path.exists(self.path):
            os.remove(self.path)

    def writeTestJsonFile(self, contents):
        with open(self.path, 'w') as f:
            f.write(json.dumps(contents))
        return os.path.abspath(self.path)

    def test_Initialize(self):
        m = Mock(spec=Manifest)
        m.GetJob.side_effect = lambda id : {
            "Id": 1,
            "RequiredS3Data": [ "document", "document1", "document3" ],
            "Commands": [
                { "Command": "run1.exe", "Args": [ ] },
                { "Command": "run2.exe", "Args": [ "a" ] }
            ]
        }
        m.GetS3Documents.side_effect = lambda : [
            {
            "Name": "document",
            "Direction": "Static",
            "LocalPath": ".",
            "AWSInstancePath": "awsinstancepath"
            },
            {
            "Name": "document1",
            "Direction": "LocalToAWS",
            "LocalPath": ".",
            "AWSInstancePath": "awsinstancepath"
            },
            {
            "Name": "document3",
            "Direction": "AWSToLocal",
            "LocalPath": ".",
            "AWSInstancePath": "awsinstancepath"
            }
        ]

        fac = InstanceMetadataFactory(m)

        instanceMeta = fac.InitializeMetadata(1, 10)
        self.assertEquals(instanceMeta.Get("Id"), 1)
        self.assertEquals(instanceMeta.Get("AWS_Instance_Id"), 10)
        self.assertEquals(instanceMeta.Get("CommandCount"), 2)
        self.assertEquals(instanceMeta.Get("UploadCount"), 1)
        self.assertEquals(instanceMeta.Get("DownloadCount"), 2)
        self.assertEquals(instanceMeta.Get("LastMessage"), "Initialize")
        self.assertTrue(TimeHelper.GetTimeElapsed(instanceMeta.Get("LastUpdate")) < 1.0)
        self.assertEquals(instanceMeta.Get("NCommandsFinished"), 0)
        self.assertEquals(instanceMeta.Get("NUploadsFinished"), 0)
        self.assertEquals(instanceMeta.Get("NDownloadsFinished"), 0)

        m.GetJob.assert_called_with(1)
        m.GetS3Documents.assert_any_call()


    def test_ToJson(self):
        data0 = {"a": 1, "b": 2}
        instanceMeta1 = Mock(spec=InstanceMetadata)
        instanceMeta1._doc = data0

        m = Mock(spec=Manifest)
        fac = InstanceMetadataFactory(m)
        fac.ToJson(instanceMeta1, self.path)
        with open(self.path) as f:
            data1 = json.load(f)
            self.assertEqual(data0, data1)

    def test_FromJson(self):
        self.writeTestJsonFile(
        {
        "Id": 10,
        "AWS_Instance_Id": "0x5413eef",
        "CommandCount": 2,
        "UploadCount": 30,
        "DownloadCount": 7000,
        "LastMessage": "init",
        "LastUpdate": "20171018-23:15:30 UTC",
        "NCommandsFinished": 1,
        "NUploadsFinished": 12,
        "NDownloadsFinished": 76
        })
        m = Mock(spec=Manifest)
        fac = InstanceMetadataFactory(m)
        inst = fac.FromJson(self.path)
        self.assertEquals(inst.Get("Id"), 10)
        self.assertEquals(inst.Get("AWS_Instance_Id"), "0x5413eef")
        self.assertEquals(inst.Get("CommandCount"), 2)
        self.assertEquals(inst.Get("UploadCount"), 30)
        self.assertEquals(inst.Get("DownloadCount"), 7000)
        self.assertEquals(inst.Get("LastMessage"), "init")
        self.assertEquals(inst.Get("LastUpdate"), "20171018-23:15:30 UTC")
        self.assertEquals(inst.Get("NCommandsFinished"), 1)
        self.assertEquals(inst.Get("NUploadsFinished"), 12)
        self.assertEquals(inst.Get("NDownloadsFinished"), 76)

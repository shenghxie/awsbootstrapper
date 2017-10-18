from mock import Mock, call
import unittest, os, shutil, json, datetime
from instancemetadata import InstanceMetadata
from manifest import Manifest


class InstanceMetadata_Test(unittest.TestCase):

    def setUp(self):
        self.path = os.path.join(os.getcwd(),"tests","test_metadata.json")

    def tearDown(self):
        if os.path.exists(self.path):
            os.remove(self.path)

    def writeTestJsonFile(self, contents):
        with open(self.path, 'w') as f:
            f.write(json.dumps(contents))
        return os.path.abspath(self.path)

    def test_init(self):
        dt = datetime.datetime.utcnow()
        instanceMeta = InstanceMetadata(
            id=1,
            aws_id=2,
            ncommands=10,
            nuploads=10,
            ndownloads=10,
            lastmessage="message",
            lastupdate=dt,
            ncommandsFinished=0,
            nuploadsFinished=0,
            ndownloadsFinished=0)
        self.assertEquals(instanceMeta.Get("Id"), 1)
        self.assertEquals(instanceMeta.Get("AWS_Instance_Id"), 2)
        self.assertEquals(instanceMeta.Get("CommandCount"), 10)
        self.assertEquals(instanceMeta.Get("UploadCount"), 10)
        self.assertEquals(instanceMeta.Get("DownloadCount"), 10)
        self.assertEquals(instanceMeta.Get("LastMessage"), "message")
        self.assertEquals(instanceMeta.Get("LastUpdate"), dt)
        self.assertEquals(instanceMeta.Get("NCommandsFinished"), 0)
        self.assertEquals(instanceMeta.Get("NUploadsFinished"), 0)
        self.assertEquals(instanceMeta.Get("NDownloadsFinished"), 0)

    def test_Get_raises_error_on_bad_key(self):
        instanceMeta = InstanceMetadata(1,2,3,4,5,6,7,8,9,10)
        self.assertRaises(KeyError,
                          lambda : instanceMeta.Get("missing"))

    def test_AllTasksFinished(self):
        instanceMeta = InstanceMetadata(
            id=1,
            aws_id=2,
            ncommands=10,
            nuploads=10,
            ndownloads=10,
            lastmessage="message",
            lastupdate=datetime.datetime.utcnow(),
            ncommandsFinished=0,
            nuploadsFinished=0,
            ndownloadsFinished=0)

        for x in range(0,10):
            self.assertFalse(instanceMeta.AllTasksFinished())
            instanceMeta.IncrementUploadsFinished()
            instanceMeta.IncrementCommandFinished()
            instanceMeta.IncrementDownloadFinished()

        self.assertTrue(instanceMeta.AllTasksFinished())

    def test_TotalTasks(self):
        instanceMeta = InstanceMetadata(
            id=1,
            aws_id=2,
            ncommands=10,
            nuploads=10,
            ndownloads=10,
            lastmessage="message",
            lastupdate=datetime.datetime.utcnow(),
            ncommandsFinished=0,
            nuploadsFinished=0,
            ndownloadsFinished=0)

        self.assertEqual(instanceMeta.TotalTasks(), 30)

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


        instanceMeta = InstanceMetadata.Initialize(1, 10, m)
        self.assertEquals(instanceMeta.Get("Id"), 1)
        self.assertEquals(instanceMeta.Get("AWS_Instance_Id"), 10)
        self.assertEquals(instanceMeta.Get("CommandCount"), 2)
        self.assertEquals(instanceMeta.Get("UploadCount"), 1)
        self.assertEquals(instanceMeta.Get("DownloadCount"), 2)
        self.assertEquals(instanceMeta.Get("LastMessage"), "Initialize")
        self.assertTrue(
            (datetime.datetime.utcnow() - 
            InstanceMetadata.ParseUTCString(instanceMeta.Get("LastUpdate")))
            .total_seconds() < 1.0)
        self.assertEquals(instanceMeta.Get("NCommandsFinished"), 0)
        self.assertEquals(instanceMeta.Get("NUploadsFinished"), 0)
        self.assertEquals(instanceMeta.Get("NDownloadsFinished"), 0)



    def test_UpdateMessage(self):
        instanceMeta = InstanceMetadata(
            id=1,
            aws_id=2,
            ncommands=10,
            nuploads=10,
            ndownloads=10,
            lastmessage="message",
            lastupdate="empty",
            ncommandsFinished=0,
            nuploadsFinished=0,
            ndownloadsFinished=0)

        self.assertEqual(instanceMeta.Get("LastUpdate"), "empty")
        self.assertEqual(instanceMeta.Get("LastMessage"), "message")
        
        instanceMeta.UpdateMessage("newMessage")
        
        self.assertEqual(instanceMeta.Get("LastMessage"), "newMessage")
        newTime = instanceMeta.Get("LastUpdate")
        self.assertTrue(
            (datetime.datetime.utcnow() - 
             InstanceMetadata.ParseUTCString(newTime))
            .total_seconds() < 1.0)

    def test_IncrementUploadsFinished(self):
        instanceMeta = InstanceMetadata(
            id=1,
            aws_id=2,
            ncommands=10,
            nuploads=10,
            ndownloads=10,
            lastmessage="message",
            lastupdate="empty",
            ncommandsFinished=0,
            nuploadsFinished=0,
            ndownloadsFinished=0)

        self.assertEqual(instanceMeta.Get("NUploadsFinished"), 0)
        self.assertEqual(instanceMeta.Get("LastUpdate"), "empty")
        instanceMeta.IncrementUploadsFinished()
        self.assertEqual(instanceMeta.Get("NUploadsFinished"), 1)
        newTime = instanceMeta.Get("LastUpdate")
        self.assertTrue(
            (datetime.datetime.utcnow() - 
             InstanceMetadata.ParseUTCString(newTime))
            .total_seconds() < 1.0)

    def test_IncrementCommandFinished(self):
        instanceMeta = InstanceMetadata(
            id=1,
            aws_id=2,
            ncommands=10,
            nuploads=10,
            ndownloads=10,
            lastmessage="message",
            lastupdate="empty",
            ncommandsFinished=0,
            nuploadsFinished=0,
            ndownloadsFinished=0)

        self.assertEqual(instanceMeta.Get("NCommandsFinished"), 0)
        self.assertEqual(instanceMeta.Get("LastUpdate"), "empty")
        instanceMeta.IncrementCommandFinished()
        self.assertEqual(instanceMeta.Get("NCommandsFinished"), 1)
        newTime = instanceMeta.Get("LastUpdate")
        self.assertTrue(
            (datetime.datetime.utcnow() - 
             InstanceMetadata.ParseUTCString(newTime))
            .total_seconds() < 1.0)

    def test_IncrementDownloadFinished(self):
        instanceMeta = InstanceMetadata(
            id=1,
            aws_id=2,
            ncommands=10,
            nuploads=10,
            ndownloads=10,
            lastmessage="message",
            lastupdate="empty",
            ncommandsFinished=0,
            nuploadsFinished=0,
            ndownloadsFinished=0)

        self.assertEqual(instanceMeta.Get("NDownloadsFinished"), 0)
        self.assertEqual(instanceMeta.Get("LastUpdate"), "empty")
        instanceMeta.IncrementDownloadFinished()
        self.assertEqual(instanceMeta.Get("NDownloadsFinished"), 1)
        newTime = instanceMeta.Get("LastUpdate")
        self.assertTrue(
            (datetime.datetime.utcnow() - 
             InstanceMetadata.ParseUTCString(newTime))
            .total_seconds() < 1.0)

    def test_GetTimeSinceLastUpdate(self):

        dummyTimeStr = "20171018-23:15:30 UTC"
        dummyTime = InstanceMetadata.ParseUTCString(dummyTimeStr)
        instanceMeta = InstanceMetadata(
            id=1,
            aws_id=2,
            ncommands=10,
            nuploads=10,
            ndownloads=10,
            lastmessage="message",
            lastupdate=dummyTimeStr,
            ncommandsFinished=0,
            nuploadsFinished=0,
            ndownloadsFinished=0)

        expectedInterval = (datetime.datetime.utcnow() - dummyTime).total_seconds()
        interval = instanceMeta.GetTimeSinceLastUpdate()

        self.assertLess(abs(expectedInterval - interval), 0.1)

    def test_ToJson(self):
        instanceMeta1 = InstanceMetadata(
            id=1,
            aws_id=2,
            ncommands=10,
            nuploads=10,
            ndownloads=10,
            lastmessage="message",
            lastupdate="20171018-23:15:30 UTC",
            ncommandsFinished=1,
            nuploadsFinished=2,
            ndownloadsFinished=3)

        InstanceMetadata.ToJson(instanceMeta1, self.path)
        instanceMeta2 = InstanceMetadata.FromJson(self.path)
        fields = [
            "Id",
            "AWS_Instance_Id",
            "CommandCount",
            "UploadCount",
            "DownloadCount",
            "LastMessage",
            "LastUpdate",
            "NCommandsFinished",
            "NUploadsFinished",
            "NDownloadsFinished"
        ]
        for f in fields:
            self.assertEqual(instanceMeta1.Get(f), instanceMeta2.Get(f))

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
        inst = InstanceMetadata.FromJson(self.path)
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

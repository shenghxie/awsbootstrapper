import unittest
from instancemetadata import InstanceMetadata
from timehelper import TimeHelper

class InstanceMetadata_Test(unittest.TestCase):

    def test_init(self):

        instanceMeta = InstanceMetadata(
            id=1,
            aws_id=2,
            ncommands=10,
            nuploads=10,
            ndownloads=10,
            lastmessage="message",
            lastupdate="2017",
            ncommandsFinished=0,
            nuploadsFinished=0,
            ndownloadsFinished=0)
        self.assertEquals(instanceMeta.Get("Id"), 1)
        self.assertEquals(instanceMeta.Get("AWS_Instance_Id"), 2)
        self.assertEquals(instanceMeta.Get("CommandCount"), 10)
        self.assertEquals(instanceMeta.Get("UploadCount"), 10)
        self.assertEquals(instanceMeta.Get("DownloadCount"), 10)
        self.assertEquals(instanceMeta.Get("LastMessage"), "message")
        self.assertEquals(instanceMeta.Get("LastUpdate"), "2017")
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
            lastupdate=" ",
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
            lastupdate="the date",
            ncommandsFinished=0,
            nuploadsFinished=0,
            ndownloadsFinished=0)

        self.assertEqual(instanceMeta.TotalTasks(), 30)

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
        self.assertTrue(TimeHelper.GetTimeElapsed(newTime) < 1.0)

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
        self.assertTrue(TimeHelper.GetTimeElapsed(newTime) < 1.0)

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
        self.assertTrue(TimeHelper.GetTimeElapsed(newTime) < 1.0)

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
        self.assertTrue(TimeHelper.GetTimeElapsed(newTime) < 1.0)

    def test_GetTimeSinceLastUpdate(self):

        dummyTimeStr = "20171018-23:15:30 UTC"
        dummyTime = TimeHelper.ParseUTCString(dummyTimeStr)
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

        expectedInterval = TimeHelper.GetTimeElapsed(dummyTimeStr)
        interval = instanceMeta.GetTimeSinceLastUpdate()

        self.assertLess(abs(expectedInterval - interval), 0.1)

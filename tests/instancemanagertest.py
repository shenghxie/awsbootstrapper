import unittest, os, shutil
from instancemanager import InstanceManager
from mock import Mock, call
from s3interface import S3Interface
from manifest import Manifest
from ec2interface import EC2Interface
from instancemetadatafactory import InstanceMetadataFactory
from instancemetadata import InstanceMetadata

class InstanceManager_Test(unittest.TestCase):

    def test_constructor(self):
        mockManifest = Mock(spec=Manifest)
        mockS3Interface = Mock(spec=S3Interface)
        mockInstanceMetaFac = Mock(spec=InstanceMetadataFactory)
        inst = InstanceManager(mockS3Interface, mockManifest, mockInstanceMetaFac)
        self.assertTrue(inst.manifest == mockManifest)
        self.assertTrue(inst.s3Interface == mockS3Interface)
        self.assertTrue(inst.instanceMetadataFactory == mockInstanceMetaFac)

    def test_GetMetaFileTempPath(self):
        mockManifest = Mock(spec=Manifest)
        mockS3Interface = Mock(spec=S3Interface)
        mockS3Interface.localTempDir = "temp123"
        mockInstanceMetadataFactory = Mock(spec=InstanceMetadataFactory)
        inst = InstanceManager(mockS3Interface, mockManifest,
                               mockInstanceMetadataFactory)
        result = inst.GetMetaFileTempPath(10)
        self.assertEqual(result,
                         os.path.join("temp123","instance_metadata10.json"))

    def test_downloadMetaData(self):
        mockManifest = Mock(spec=Manifest)
        mockManifest.GetS3KeyPrefix.side_effect = lambda : ""
        mockS3Interface = Mock(spec=S3Interface)
        mockS3Interface.localTempDir = "tests"
        mockInstanceMetadataFactory = Mock(spec=InstanceMetadataFactory)
        inst = InstanceManager(mockS3Interface, mockManifest,
                               mockInstanceMetadataFactory)

        mockInstanceMetadataFactory.FromJson.side_effect = \
            lambda tmpfile : -9999

        instId = 10
        tmpfile = inst.GetMetaFileTempPath(instId)
        with open(tmpfile, 'w') as tmp:
            tmp.write("nothing")
        res = inst.downloadMetaData(instId)

        mockInstanceMetadataFactory.FromJson.assert_called_once_with(tmpfile)
        self.assertTrue(res == -9999)
        self.assertFalse(os.path.exists(tmpfile))
        mockManifest.GetS3KeyPrefix.has_call()
        mockS3Interface.downloadFile.has_call()
        self.assertFalse(os.path.exists(tmpfile))


    def test_uploadMetaData(self):
        mockManifest = Mock(spec=Manifest)
        mockManifest.GetS3KeyPrefix.side_effect = lambda : ""
        mockS3Interface = Mock(spec=S3Interface)
        mockS3Interface.localTempDir = "tests"
        mockInstanceMetadataFactory = Mock(spec=InstanceMetadataFactory)
        mockMetadata = Mock(spec=InstanceMetadata)
        instId = 17
        mockMetadata.Get.side_effect = lambda key : {"Id": instId}[key]

        inst = InstanceManager(mockS3Interface, mockManifest,
                               mockInstanceMetadataFactory)

        tmpfile = inst.GetMetaFileTempPath(instId)
        with open(tmpfile, 'w') as tmp:
            tmp.write("nothing")

        inst.uploadMetaData(mockMetadata)

        self.assertFalse(os.path.exists(tmpfile))
        mockS3Interface.uploadFile.has_call()
        mockInstanceMetadataFactory.ToJson.assert_any_call(mockMetadata, tmpfile)

    def test_GetKeyPrefix(self):
        mockManifest = Mock(spec=Manifest)
        mockS3Interface = Mock(spec=S3Interface)
        mockInstanceMetadataFactory = Mock(spec=InstanceMetadataFactory)
        inst = InstanceManager(mockS3Interface, mockManifest,
                               mockInstanceMetadataFactory)
        mockManifest.GetS3KeyPrefix.side_effect = lambda : "the prefix"
        self.assertTrue( inst.GetKeyPrefix(1000) == "the prefix/instances/1000")
        mockManifest.GetS3KeyPrefix.assert_any_call()

    def test_publishInstance(self):

        mockManifest = Mock(spec=Manifest)
        mockS3Interface = Mock(spec=S3Interface)
        mockInstanceMetadataFactory = Mock(spec=InstanceMetadataFactory)

        mockInstanceMetadataFactory.InitializeMetadata.side_effect = \
            lambda i, a : "metadata"

        mockManifest.GetS3KeyPrefix.side_effect = lambda : "the prefix"
        mockS3Interface.localTempDir = "tests"
        inst = InstanceManager(mockS3Interface, mockManifest,
                                mockInstanceMetadataFactory)
        instId = 100
        tmpfile = inst.GetMetaFileTempPath(instId)
        with open(tmpfile, 'w') as tmp:
            tmp.write("nothing")

        inst.publishInstance(instId, 1000)

        mockManifest.GetS3KeyPrefix.assert_any_call()
        mockS3Interface.uploadFile.assert_called_with(
            tmpfile, "the prefix/instances/100/metadata.json", True)
        mockInstanceMetadataFactory.InitializeMetadata.assert_any_call(instId,1000)
        mockInstanceMetadataFactory.ToJson.assert_any_call("metadata",tmpfile)
        self.assertFalse(os.path.exists(tmpfile))

    def test_uploadInstanceData(self):
        mockManifest = Mock(spec=Manifest)
        mockS3Interface = Mock(spec=S3Interface)
        mockManifest.GetS3KeyPrefix.side_effect = lambda : "the prefix"
        mockInstanceMetadataFactory = Mock(spec=InstanceMetadataFactory)
        inst = InstanceManager(mockS3Interface, mockManifest,
                               mockInstanceMetadataFactory)
        inst.uploadInstanceData(10000, "docname", "localPath")
        mockManifest.GetS3KeyPrefix.assert_any_call()
        mockS3Interface.uploadFile.assert_called_with(
                "localPath",
                "the prefix/instances/10000/docname", True)

    def test_downloadInstanceData(self):
        mockManifest = Mock(spec=Manifest)
        mockManifest.GetS3KeyPrefix.side_effect = lambda : "the prefix"
        mockS3Interface = Mock(spec=S3Interface)
        mockInstanceMetadataFactory = Mock(spec=InstanceMetadataFactory)
        inst = InstanceManager(mockS3Interface, mockManifest,
                               mockInstanceMetadataFactory)
        inst.uploadInstanceData(10000, "docname", "localPath")
        mockS3Interface.uploadFile.assert_has_calls(
            [call("localPath", 
                  "the prefix/instances/10000/docname", 
                  True)])

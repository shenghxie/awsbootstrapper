import unittest, os, shutil
from instancemanager import InstanceManager
from mock import Mock, call
from s3interface import S3Interface
from manifest import Manifest
from ec2interface import EC2Interface

class InstanceManager_Test(unittest.TestCase):

    def test_constructor(self):
        mockManifest = Mock(spec=Manifest)
        mockS3Interface = Mock(spec=S3Interface)
        inst = InstanceManager(mockS3Interface, mockManifest)
        self.assertTrue(inst.manifest == mockManifest)
        self.assertTrue(inst.s3Interface == mockS3Interface)

    def test_GetMetadata(self):
        mockManifest = Mock(spec=Manifest)
        mockS3Interface = Mock(spec=S3Interface)
        inst = InstanceManager(mockS3Interface, mockManifest)
        ec2inst = Mock(spec=EC2Interface)
        ec2inst.instance_id = "999"

        self.assertTrue( inst.GetMetadata(ec2inst) == { "instance_id": "999" })

    def test_GetKeyPrefix(self):
        mockManifest = Mock(spec=Manifest)
        mockS3Interface = Mock(spec=S3Interface)
        inst = InstanceManager(mockS3Interface, mockManifest)
        mockManifest.GetS3KeyPrefix.side_effect = lambda : "the prefix"
        self.assertTrue( inst.GetKeyPrefix(1000) == "the prefix/instances/1000")
        mockManifest.GetS3KeyPrefix.assert_any_call()

    def test_publishInstance(self):
        tempPath = os.path.join(os.getcwd(), "tests", "temp")
        os.makedirs(tempPath)
        try:
            mockManifest = Mock(spec=Manifest)
            mockS3Interface = Mock(spec=S3Interface)
            mockec2inst = Mock(spec=EC2Interface)
            mockec2inst.instance_id = "100"
            mockManifest.GetS3KeyPrefix.side_effect = lambda : "the prefix"

            mockS3Interface.localTempDir = tempPath

            inst = InstanceManager(mockS3Interface, mockManifest)
            inst.publishInstance(100, mockec2inst)

            mockManifest.GetS3KeyPrefix.assert_any_call()
            mockS3Interface.uploadFile.assert_called_with(
                os.path.join(tempPath, "instance_metadata{0}.json".format(100)),
                "the prefix/instances/100/metadata.json", True)
        finally:
            shutil.rmtree(tempPath)

    def test_uploadInstanceData(self):
        mockManifest = Mock(spec=Manifest)
        mockS3Interface = Mock(spec=S3Interface)
        mockManifest.GetS3KeyPrefix.side_effect = lambda : "the prefix"

        inst = InstanceManager(mockS3Interface, mockManifest)
        inst.uploadInstanceData(10000, "docname", "localPath")
        mockManifest.GetS3KeyPrefix.assert_any_call()
        mockS3Interface.uploadFile.assert_called_with(
                "localPath",
                "the prefix/instances/10000/docname", True)

    def test_downloadInstanceData(self):
        mockManifest = Mock(spec=Manifest)
        mockManifest.GetS3KeyPrefix.side_effect = lambda : "the prefix"
        mockS3Interface = Mock(spec=S3Interface)

        inst = InstanceManager(mockS3Interface, mockManifest)
        inst.uploadInstanceData(10000, "docname", "localPath")
        mockS3Interface.uploadFile.assert_has_calls(
            [call("localPath", 
                  "the prefix/instances/10000/docname", 
                  True)])

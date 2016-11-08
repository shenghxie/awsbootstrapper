import unittest, os, shutil
from instancemanager import InstanceManager
class MockS3Interface(object):

    def __init__(self):
        self.downloadFileMock = lambda  keyName, localPath: None
        self.uploadFileMock = lambda localPath, keyName: None

    def downloadFile(self, keyName, localPath):
        return self.downloadFileMock(keyName, localPath)

    def uploadFile(self, localPath, keyName):
        return self.uploadFileMock(localPath, keyName)

class MockManifest(object):
    
    def __init__(self):
        self.GetS3KeyPrefixOverride = lambda : None

    def GetS3KeyPrefix(self):
        return self.GetS3KeyPrefixOverride()

class MockEC2Instance(object):
    def __init__(self):
        pass
class InstanceManager_Test(unittest.TestCase):

    def test_constructor(self):
        mockManifest = MockManifest()
        mockS3Interface = MockS3Interface()
        inst = InstanceManager(mockS3Interface, mockManifest)
        self.assertTrue(inst.manifest == mockManifest)
        self.assertTrue(inst.s3Interface == mockS3Interface)

    def test_GetMetadata(self):
        mockManifest = MockManifest()
        mockS3Interface = MockS3Interface()
        inst = InstanceManager(mockS3Interface, mockManifest)
        ec2inst = MockEC2Instance()
        ec2inst.instance_id = "999"

        self.assertTrue( inst.GetMetadata(ec2inst) == { "instance_id": "999" })

    def test_GetKeyPrefix(self):
        mockManifest = MockManifest()
        def GetS3KeyPrefix():
            return "the prefix"
        mockManifest.GetS3KeyPrefixOverride = GetS3KeyPrefix

        mockS3Interface = MockS3Interface()
        inst = InstanceManager(mockS3Interface, mockManifest)
        self.assertTrue( inst.GetKeyPrefix(1000) == "the prefix/instances/1000")

    def test_publishInstance(self):
        tempPath = os.path.join(os.getcwd(), "tests", "temp")
        os.makedirs(tempPath)
        try:
            mockManifest = MockManifest()
            def GetS3KeyPrefix():
                return "the prefix"
            mockManifest.GetS3KeyPrefixOverride = GetS3KeyPrefix
            mockS3Interface = MockS3Interface()
            def uploadFile(localPath, keyName):
                self.assertTrue(localPath == os.path.join(tempPath, 
                                         "instance_metadata{0}.json".format(100)))
                self.assertTrue(keyName == "the prefix/instances/100/metadata.json")

            mockS3Interface.uploadFileMock = uploadFile
            mockS3Interface.localTempDir = tempPath
            ec2inst = MockEC2Instance()
            ec2inst.instance_id = "999"
            ec2inst.launch_time = "2016/11/03"
            inst = InstanceManager(mockS3Interface, mockManifest)
            inst.publishInstance(100, ec2inst)
        finally:
            shutil.rmtree(tempPath)

    def test_uploadInstanceData(self):
        mockManifest = MockManifest()
        def GetS3KeyPrefix():
            return "the prefix"
        mockManifest.GetS3KeyPrefixOverride = GetS3KeyPrefix
        mockS3Interface = MockS3Interface()
        def uploadFile(localPath, keyName):
            self.assertTrue(localPath == "localPath")
            self.assertTrue(keyName == "the prefix/instances/10000/docname")

        mockS3Interface.uploadFileMock = uploadFile
        inst = InstanceManager(mockS3Interface, mockManifest)
        inst.uploadInstanceData(10000, "docname", "localPath")

    def test_downloadInstanceData(self):
        mockManifest = MockManifest()
        def GetS3KeyPrefix():
            return "the prefix"
        mockManifest.GetS3KeyPrefixOverride = GetS3KeyPrefix
        mockS3Interface = MockS3Interface()
        def downloadloadFile(keyName, localPath):
            self.assertTrue(localPath == "localPath")
            self.assertTrue(keyName == "the prefix/instances/10000/docname")

        mockS3Interface.downloadFileMock = downloadloadFile
        inst = InstanceManager(mockS3Interface, mockManifest)
        inst.uploadInstanceData(10000, "docname", "localPath")

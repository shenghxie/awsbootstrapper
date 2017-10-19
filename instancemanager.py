import json, os
from instancemetadatafactory import InstanceMetadataFactory
from loghelper import LogHelper
class InstanceManager(object):

    def __init__(self, s3Interface, manifest, instanceMetadataFactory):
        self.s3Interface = s3Interface
        self.manifest = manifest
        self.instanceMetadataFactory = instanceMetadataFactory
        self.__metadataFileName = "metadata.json"

    def GetKeyPrefix(self, instanceId):
        return "/".join([self.manifest.GetS3KeyPrefix(),
                         "instances",str(instanceId)])

    def GetMetaFileTempPath(self, instanceId):
        localMetaFile = os.path.join(self.s3Interface.localTempDir,
                                "instance_metadata{0}.json"
                                .format(instanceId))
        return localMetaFile

    def publishInstance(self, instanceId, awsInstanceId):
        """called at the moment that an instance is created
        uploads instance metadata to s3"""

        localMetaFile = self.GetMetaFileTempPath(instanceId)
        metadata = self.instanceMetadataFactory.InitializeMetadata(
            instanceId,
            awsInstanceId)
        self.instanceMetadataFactory.ToJson(metadata, localMetaFile)
        self.uploadInstanceData(instanceId, self.__metadataFileName,
                                localMetaFile)
        os.remove(localMetaFile)

    def downloadMetaData(self, instanceId):
        localMetaFile = self.GetMetaFileTempPath(instanceId)
        self.downloadInstanceData(instanceId, self.__metadataFileName,
                                  localMetaFile, False)
        inst = self.instanceMetadataFactory.FromJson(localMetaFile)
        os.remove(localMetaFile)
        return inst

    def uploadMetaData(self, metadata):
        instanceId = metadata.Get("Id")
        localMetaFile = self.GetMetaFileTempPath(instanceId)
        self.instanceMetadataFactory.ToJson(metadata, localMetaFile)
        self.uploadInstanceData(instanceId, self.__metadataFileName,
                                localMetaFile, False)
        os.remove(localMetaFile)

    def uploadInstanceLog(self, instanceId):
        """
        uploads the log file to the s3 key for the specified instance
        """
        s3DocumentName = LogHelper.instanceLogFileName(instanceId)
        instanceLogPath = LogHelper.instanceLogPath(
            self.s3Interface.localTempDir, instanceId)
        self.uploadInstanceData(
            instanceId, s3DocumentName, instanceLogPath, False)
        
    def downloadInstanceLog(self, instanceId, localDir):
        s3DocumentName = LogHelper.instanceLogFileName(instanceId)
        self.downloadInstanceData(instanceId, s3DocumentName, 
                                  os.path.join(localDir, s3DocumentName))

    def uploadInstanceData(self, instanceId, s3DocumentName, localPath, logged=True):
        key = self.GetKeyPrefix(instanceId)
        key = "/".join([key, s3DocumentName])
        self.s3Interface.uploadFile(localPath, key, logged)

    def downloadInstanceData(self, instanceId, s3DocumentName, localPath, logged=True):
        key = self.GetKeyPrefix(instanceId)
        key = "/".join([key, s3DocumentName])
        self.s3Interface.downloadFile(key, localPath, logged)


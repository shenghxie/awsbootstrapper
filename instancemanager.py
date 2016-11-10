import json, os
class InstanceManager(object):

    def __init__(self, s3Interface, manifest):
        self.s3Interface = s3Interface
        self.manifest = manifest
        self.__metadataFileName = "metadata.json"

    def GetMetadata(self, instance):
        return {
            "instance_id": instance.instance_id,
            #"launch_time": instance.launch_time,
        }

    def GetKeyPrefix(self, instanceId):
        return "/".join([self.manifest.GetS3KeyPrefix(),
                         "instances",str(instanceId)])

    def publishInstance(self, instanceId, instance):
        """called at the moment that an instance is created
        uploads instance metadata to s3"""

        localMetaFile = os.path.join(self.s3Interface.localTempDir, 
                                     "instance_metadata{0}.json"
                                     .format(instanceId))
        metadata = self.GetMetadata(instance)
        with open(localMetaFile, 'w') as f:
            f.write(json.dumps(metadata))
        self.uploadInstanceData(instanceId, self.__metadataFileName, localMetaFile)
        os.remove(localMetaFile)

    def uploadInstanceData(self, instanceId, s3DocumentName, localPath):
        key = self.GetKeyPrefix(instanceId)
        key = "/".join([key, s3DocumentName])
        self.s3Interface.uploadFile(localPath, key)

    def downloadInstanceData(self, instanceId, s3DocumentName, localPath):
        key = self.GetKeyPrefix(instanceId)
        key = "/".join([key, s3DocumentName])
        self.s3Interface.downloadFile(key, localPath)


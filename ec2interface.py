import os, logging
from manifest import Manifest
class EC2Interface(object):
    
    def __init__(self, ec2Resource, instanceLocalWorkingDir, manifest, 
                 manifestKey, instanceManager, bootstrapScriptPath,
                 bootstrapCommandFormat):
        self.ec2Resource = ec2Resource
        self.instanceLocalWorkingDir = instanceLocalWorkingDir
        self.manifest = manifest
        self.manifestKey = manifestKey
        self.instanceManager = instanceManager
        self.bootstrapScriptPath = bootstrapScriptPath
        self.bootstrapCommandFormat = bootstrapCommandFormat

    def launchInstance(self, config):
        instance = self.ec2Resource.create_instances(**config)
        return instance[0]

    def buildBootstrapCommand(self, instanceId):
        bootstrapperCommand = ("'{scriptPath}' "+
               "--bucketName {bucketName} "+
               "--manifestKey {manifestKey} "+
               "--instanceId {instanceId} "+
               "--localWorkingDir '{localWorkingDir}'").format(
                   scriptPath=self.bootstrapScriptPath,
                   bucketName=self.manifest.GetBucketName(),
                   manifestKey=self.manifestKey,
                   instanceId=instanceId,
                   localWorkingDir=self.instanceLocalWorkingDir)

        return self.bootstrapCommandFormat.format(bootstrapperCommand)

    def launchInstances(self, config):
        instances = { }
        ordered_instance_ids = []
        for job in self.manifest.GetJobs():
            id = job["Id"]
            userdata = self.buildBootstrapCommand(id)
            config["UserData"] = userdata
            config["MinCount"] = 1
            config["MaxCount"] = 1
            logging.info("launching instance {0}".format(config))
            instance = self.launchInstance(config)
            
            instances[id] = instance
            ordered_instance_ids.append(id)

        allRunning = False
        for i in ordered_instance_ids:
            instances[i].wait_until_running()
            instances[i].create_tags(
                Tags=[
                    { "Key": "Name", "Value": "{0}[{1}]".format(self.manifest.GetS3KeyPrefix(), i)}
                ])
            self.instanceManager.publishInstance(i, instances[i])
            logging.info("instance {0} successfully created AWS ID: {1}".format(i, instances[i].instance_id))

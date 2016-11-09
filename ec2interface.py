import os, logging
from manifest import Manifest
class EC2Interface(object):
    
    def __init__(self, ec2Resource, instanceLocalWorkingDir, manifest, 
                 manifestKey, instanceManager, pythonpath, bootstrapScriptPath,
                 lineBreak, bootstrapCommands):
        self.ec2Resource = ec2Resource
        self.instanceLocalWorkingDir = instanceLocalWorkingDir
        self.manifest = manifest
        self.manifestKey = manifestKey
        self.instanceManager = instanceManager
        self.pythonpath = pythonpath
        self.bootstrapScriptPath = bootstrapScriptPath
        self.lineBreak = lineBreak
        self.bootstrapCommands = bootstrapCommands

    def launchInstance(self, config):
        """launches an EC2 Instance base on the specified config
        config -- a dictionary that will be converted to keyword args for the boto3 create_instances function
        
        see: http://boto3.readthedocs.io/en/latest/reference/services/ec2.html#EC2.ServiceResource.create_instances
        """
        instance = self.ec2Resource.create_instances(**config)
        return instance[0]

    def buildBootstrapCommand(self, instanceId):
        bootstrapperCommand = ("{pythonpath} '{scriptPath}' "+
               "--bucketName '{bucketName}' "+
               "--manifestKey '{manifestKey}' "+
               "--instanceId {instanceId} "+
               "--localWorkingDir '{localWorkingDir}'").format(
                   pythonpath=self.pythonpath,
                   scriptPath=self.bootstrapScriptPath,
                   bucketName=self.manifest.GetBucketName(),
                   manifestKey=self.manifestKey,
                   instanceId=instanceId,
                   localWorkingDir=self.instanceLocalWorkingDir)

        finalCommands = list(self.bootstrapCommands)
        for index,cmd in enumerate(finalCommands):
            if cmd == "$BootStrapScript":
                finalCommands[index] = bootstrapperCommand
        return self.lineBreak.join(finalCommands)

    def launchInstances(self, config):
        instances = { }
        ordered_instance_ids = []
        jobs = self.manifest.GetJobs()
        for job in jobs:
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

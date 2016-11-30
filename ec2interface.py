import os, logging
from manifest import Manifest
class EC2Interface(object):
    
    def __init__(self, ec2Resource, instanceLocalWorkingDir, manifest, 
                 manifestKey, instanceManager, pythonpath, bootstrapScriptPath,
                 lineBreak, bootstrapCommands):
        """
        Args:
            ec2Resource: boto3 ec2 resource used to issue commands to the AWS 
            EC2 service
            
            instanceLocalWorkingDir: path to a writeable directory on instances
            to be launched for temporary working files
            
            manifest: Manifest instance storing data specific to each instance
            
            manifestKey: The S3 key for the manifest file which is required by 
            instances to download the manifest locally
            
            instanceManager: object for publishing instance-specific data to s3
            
            pythonpath: the path to the python interpreter on instances
            
            bootstrapScriptPath: the path to the awsbootstrap script on 
            instances

            lineBreak: the line break symbol appropriate for the instance 
            operating system

            bootstrapCommands: the list of commands to issue on instance 
            startup.  The magic name "$BootStrapScript" represents the
            position in the commands where the awsbootstrap script will be run
        """
        self.ec2Resource = ec2Resource
        self.instanceLocalWorkingDir = instanceLocalWorkingDir
        self.manifest = manifest
        self.manifestKey = manifestKey
        self.instanceManager = instanceManager
        self.pythonpath = pythonpath
        self.bootstrapScriptPath = bootstrapScriptPath
        self.lineBreak = lineBreak
        self.bootstrapCommands = bootstrapCommands
        self.__bootStrapScriptMagicName = "$BootStrapScript"

    def launchInstance(self, config):
        """launches an EC2 Instance base on the specified config
           
        Args:
            config: a dictionary that will be converted to keyword args for the
            boto3 create_instances function. see: 
            http://boto3.readthedocs.io/en/latest/reference/services/ec2.html#EC2.ServiceResource.create_instances
        Returns:
            A command to run the awsinstancebootstrapper.py script customized 
            for the targetted instance
        """
        instance = self.ec2Resource.create_instances(**config)
        return instance[0]

    def buildBootstrapCommand(self, instanceId):
        """returns a aws bootstrap command specific to the instance id being 
        launched, along with the other commands specified in bootstrapCommands
        
        Args:
            instanceId: the id of the instance, as defined in the manifest, for
            which to build the bootstrap commands
        """
        bootstrapperCommand = ('{pythonpath} "{scriptPath}" '+
               '--bucketName "{bucketName}" '+
               '--manifestKey "{manifestKey}" '+
               '--instanceId {instanceId} '+
               '--localWorkingDir "{localWorkingDir}"').format(
                   pythonpath=self.pythonpath,
                   scriptPath=self.bootstrapScriptPath,
                   bucketName=self.manifest.GetBucketName(),
                   manifestKey=self.manifestKey,
                   instanceId=instanceId,
                   localWorkingDir=self.instanceLocalWorkingDir)

        #copy the command list so this instance's list wont be modified
        cmdList = list(self.bootstrapCommands)
        for index,cmd in enumerate(cmdList):
            if cmd == self.__bootStrapScriptMagicName:
                cmdList[index] = bootstrapperCommand
        #add the line break symbol between each command item
        return self.lineBreak.join(cmdList)

    def launchInstances(self, config):
        """launches an AWS EC2 instance for each job specified in the manifest.
        The startup command is built specifically for each instance launched, 
        based on the job configuration in the manifest.

        Args:
            config: a dictionary that will be converted to keyword args for the
            boto3 create_instances function. see: 
            http://boto3.readthedocs.io/en/latest/reference/services/ec2.html#EC2.ServiceResource.create_instances
        """
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

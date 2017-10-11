import logging, os
from ec2interface import EC2Interface
from s3interface import S3Interface
from manifest import Manifest
from instancemanager import InstanceManager

class Application(object):
    
    def __init__(self, s3, manifestPath, localWorkingDir):
        self.manifestPath = manifestPath
        self.manifest = Manifest(manifestPath)
        self.s3interface = S3Interface(s3, self.manifest.GetBucketName(), localWorkingDir)
        self.instanceManager = InstanceManager(self.s3interface, self.manifest)
        self.manifestKey = "/".join([self.manifest.GetS3KeyPrefix(), "manifest.json"])

    def downloadS3Documents(self):
        logging.info("downloading files from s3 bucket {0}".format(self.s3interface.bucketName))

        for doc in self.manifest.GetS3Documents(filter = {"Direction": "AWSToLocal"}):
            self.s3interface.downloadCompressed(self.manifest.GetS3KeyPrefix(), doc["Name"],
                                                os.path.abspath(doc["LocalPath"]))

    def uploadS3Documents(self):
        logging.info("uploading files to s3 bucket {0}".format(self.s3interface.bucketName))
        logging.info("uploading manifest {0} to {1}".format(self.manifestPath, self.manifestKey))
        self.s3interface.uploadFile(self.manifestPath, self.manifestKey)

        for doc in self.manifest.GetS3Documents(filter = {"Direction": "LocalToAWS"}):
            self.s3interface.uploadCompressed(self.manifest.GetS3KeyPrefix(), doc["Name"],
                                              os.path.abspath(doc["LocalPath"]))

    def runInstances(self, ec2, instanceConfig):
        ec2interface = EC2Interface(ec2, instanceConfig["BootStrapperConfig"]["WorkingDirectory"], 
                                     self.manifest, self.manifestKey, self.instanceManager,
                                     instanceConfig["BootStrapperConfig"]["PythonPath"],
                                     instanceConfig["BootStrapperConfig"]["BootStrapScriptPath"],
                                     instanceConfig["BootStrapperConfig"]["LineBreak"],
                                     instanceConfig["BootStrapperConfig"]["BootstrapCommands"])
        ec2interface.launchInstances(instanceConfig["EC2Config"]["InstanceConfig"])
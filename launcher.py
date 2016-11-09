import boto3, logging
from ec2interface import EC2Interface
from s3interface import S3Interface
from manifest import Manifest
from instancemanager import InstanceManager

class Launcher(object):
    
    def __init__(self, instanceConfig, manifestPath, localWorkingDir):
        self.manifestPath = manifestPath
        self.manifest = Manifest(manifestPath)
        self.instanceConfig = instanceConfig
        s3 = boto3.resource('s3')
        ec2 = boto3.resource('ec2', region_name=instanceConfig["EC2Config"]["Region"])
        self.s3interface = S3Interface(s3, self.manifest.GetBucketName(), localWorkingDir)
        self.instanceManager = InstanceManager(self.s3interface, self.manifest)
        self.manifestKey = "/".join([self.manifest.GetS3KeyPrefix(), "manifest.json"])
        self.ec2interface = EC2Interface(ec2, instanceConfig["BootStrapperConfig"]["WorkingDirectory"], 
                                         self.manifest, self.manifestKey, self.instanceManager,
                                         instanceConfig["BootStrapperConfig"]["PythonPath"],
                                         instanceConfig["BootStrapperConfig"]["BootStrapScriptPath"],
                                         instanceConfig["BootStrapperConfig"]["LineBreak"],
                                         instanceConfig["BootStrapperConfig"]["BootstrapCommands"])

    def uploadS3Documents(self):
        logging.info("uploading files to s3 bucket {0}".format(self.s3interface.bucketName))
        logging.info("uploading manifest {0} to {1}".format(self.manifestPath, self.manifestKey))
        self.s3interface.uploadFile(self.manifestPath, self.manifestKey)

        for doc in self.manifest.GetS3Documents(filter = {"Direction": "LocalToAWS"}):
            self.s3interface.uploadCompressed(self.manifest.GetS3KeyPrefix(), doc["Name"], doc["LocalPath"])

    def runInstances(self):
        self.ec2interface.launchInstances(self.instanceConfig["EC2Config"]["InstanceConfig"])

def main():

    import argparse, json, os, sys
    from loghelper import LogHelper
    LogHelper.start_logging("launcher.log")
    parser = argparse.ArgumentParser(
        description="AWS bootstrapper launcher" +
                    "Uploades manifest which contains data and commands to run on specified instances,"+
                    "initializes and launches instances with jobs specified in manifest")

    parser.add_argument("--manifestPath", help = "path to a manifest file describing the jobs and data requirements for the application", required=True)
    parser.add_argument("--instanceConfigPath", help = "file path to a json file with EC2 instance configuration", required=True)
    parser.add_argument("--localWorkingDir", help = "path to dir with read/write for processing data for upload and download to AWS S3", required=True)
    try:
        args = vars(parser.parse_args())
        manifestPath = args["manifestPath"]
        instanceConfigPath = args["instanceConfigPath"]
        localWorkingDir = args["localWorkingDir"]
        instanceConfig = {}
        with open(instanceConfigPath) as f:
            instanceConfig = json.load(f)
        launcher = Launcher(instanceConfig, manifestPath, localWorkingDir)
        launcher.uploadS3Documents()
        launcher.runInstances()
    except Exception as ex:
        logging.exception("error in launcher")
        sys.exit(-1)

if __name__ == "__main__":
    main()


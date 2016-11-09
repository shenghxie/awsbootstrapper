import os, subprocess, logging


class AWSInstanceBootStrapper(object):
    # this class is to be executed on an aws instance
    # it consumes the manifest and downloads files from 
    # s3 accordingly, and then runs specified commands depending on the instance id
    def __init__(self, instanceId, manifest, s3interface, instancemanager, logpath):
        self.instanceId = instanceId
        self.manifest = manifest
        self.s3interface = s3interface
        self.instancemanager = instancemanager
        self.logpath = logpath
        self.job = self.manifest.GetJob(self.instanceId)
        self.requiredS3Docs = self.job["RequiredS3Data"]

    def UploadLog(self):
        self.instancemanager.uploadInstanceData(
            self.instanceId, os.path.split(self.logpath)[1], self.logpath) 

    def DownloadS3Documents(self):
        try:
            for documentName in self.requiredS3Docs:
                documentData = self.manifest.GetS3Documents(
                    filter={ "Name": documentName })[0]
                # [0] because only one document will match because 
                # Name is constrained as a unique identifier

                #AWSInstance path is the local path on an instance
                localPath = documentData["AWSInstancePath"]
                docName = documentData["Name"]
                keyPrefix = self.manifest.GetS3KeyPrefix()

                direction = documentData["Direction"]
                if direction in ["LocalToAWS", "Static"]:
                    self.s3interface.downloadCompressed(keyPrefix, docName, localPath)
        finally:
            #upload the log after the s3downloads (which will have made log entries)
            self.UploadLog()

    def RunCommands(self):
        commandSummary = []

        for c in self.job["Commands"]:
            command = [c["Command"]]
            args = c["Args"]
            command.extend(args)
            commandSummary.append(
                {
                    "command": command,
                    "result": None,
                    "exception": None
                })

            try:
                # http://stackoverflow.com/questions/16198546/get-exit-code-and-stderr-from-subprocess-call
                logging.info("issue command: {0}".format(command))
                cmnd_output = subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True, universal_newlines=True);
                commandSummary[-1]["result"] = 0
            except subprocess.CalledProcessError as ex:
                logging.exception("error occurred running command")
                commandSummary[-1]["result"] = ex.returncode
                commandSummary[-1]["exception"] = ex.output
                logging.error(ex.output)
            finally:
                self.UploadLog()
        
        return commandSummary

    def UploadS3Documents(self):
        try:
            for documentName in self.requiredS3Docs:
                documentData = self.manifest.GetS3Documents(
                    filter={ "Name": documentName })[0]
                # [0] because exactly one document should match because 
                # Name is constrained as a unique identifier

                #AWSInstance path is the local path on an instance
                localPath = documentData["AWSInstancePath"]
                docName = documentData["Name"]
                keyPrefix = self.manifest.GetS3KeyPrefix()

                direction = documentData["Direction"]
                if direction in ["AWSToLocal"]:
                    self.s3interface.uploadCompressed(keyPrefix, docName, localPath)
        finally:
            #upload the log after the s3 uploads (which will have made log entries)
            self.UploadLog()

def main():
    """to be run on by each instance as a startup command"""
    import boto3, argparse, sys
    from s3interface import S3Interface
    from manifest import Manifest
    from instancemanager import InstanceManager
    from loghelper import LogHelper
    parser = argparse.ArgumentParser(
        description="AWS Instance bootstrapper" +
                    "Loads manifest which contains data and commands to run on this instance,"+
                    "downloads data from S3, runs commands, and uploads results to S3")

    parser.add_argument("--bucketName", help = "the name of the S3 bucket to work with", required=True)
    parser.add_argument("--manifestKey", help = "the key pointing to the manifest file in the s3 bucket", required=True)
    parser.add_argument("--instanceId", help = "the id of this instance as defined in the manifest file", required=True)
    parser.add_argument("--localWorkingDir", help = "a directory to store working files, it will be created if it does not exist on the instance", required=True)

    try:
        args = vars(parser.parse_args())
        bootstrapper = None

        bucketName = args["bucketName"]
        manifestKey = args["manifestKey"]
        instanceId = int(args["instanceId"])
        localWorkingDir = args["localWorkingDir"]

        if not os.path.exists(localWorkingDir):
            os.makedirs(localWorkingDir)
        logPath = os.path.join(localWorkingDir, "log_instance{0}.txt".format(instanceId))
        LogHelper.start_logging(logPath)
        s3interface = S3Interface(boto3.resource('s3'), bucketName, localWorkingDir)

        localManifestPath = os.path.join(localWorkingDir, "manifest.json")
        s3interface.downloadFile(manifestKey, localManifestPath)
        manifest = Manifest(localManifestPath)
        instancemanager = InstanceManager(s3interface, manifest)
        bootstrapper = AWSInstanceBootStrapper(instanceId, manifest, s3interface, instancemanager, logPath)
        bootstrapper.DownloadS3Documents()
        commandResult = bootstrapper.RunCommands()
        bootstrapper.UploadS3Documents()
    except Exception as ex:
        logging.exception("error in bootstrapper")
        if bootstrapper is not None:
            bootstrapper.UploadLog()
        sys.exit(-1)

if __name__ == "__main__":
    main()
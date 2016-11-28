import boto3, logging, argparse, json, os, sys
from application import Application
from loghelper import LogHelper

def main():

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
        manifestPath = os.path.abspath(args["manifestPath"])
        instanceConfigPath = os.path.abspath(args["instanceConfigPath"])
        localWorkingDir = os.path.abspath(args["localWorkingDir"])
        instanceConfig = {}
        with open(instanceConfigPath) as f:
            instanceConfig = json.load(f)

        s3 = boto3.resource('s3')
        ec2 = boto3.resource('ec2', region_name=instanceConfig["EC2Config"]["Region"])

        app = Application(s3, manifestPath, localWorkingDir)
        app.uploadS3Documents()
        app.runInstances(ec2, instanceConfig)
    except Exception as ex:
        logging.exception("error in launcher")
        sys.exit(1)

if __name__ == "__main__":
    main()


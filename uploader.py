import boto3, logging, argparse, json, os, sys
from application import Application
from loghelper import LogHelper

def main():

    LogHelper.start_logging("uploader.log")
    parser = argparse.ArgumentParser(
        description="AWS bootstrapper uploader" +
                    "Uploades manifest and LocalToAWS documents described in manifest to S3")

    parser.add_argument("--manifestPath", help = "path to a manifest file describing the jobs and data requirements for the application", required=True)
    parser.add_argument("--localWorkingDir", help = "path to dir with read/write for processing data for upload and download to AWS S3", required=True)
    try:
        args = vars(parser.parse_args())
        manifestPath = os.path.abspath(args["manifestPath"])
        localWorkingDir = os.path.abspath(args["localWorkingDir"])

        s3 = boto3.resource('s3')

        app = Application(s3, manifestPath, localWorkingDir)
        app.uploadS3Documents()
    except Exception as ex:
        logging.exception("error in launcher")
        sys.exit(1)

if __name__ == "__main__":
    main()


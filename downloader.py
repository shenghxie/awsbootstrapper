import boto3, logging, argparse, os, sys
from application import Application
from loghelper import LogHelper

def main():

    LogHelper.start_logging("downloader.log")
    parser = argparse.ArgumentParser(
        description="AWS bootstrapper downloader: " +
                    "Downloads AWSToLocal documents as specified in manifest, " +
                    "or optionally the specified AWSToLocal documentName")

    parser.add_argument("--manifestPath", help = "path to a manifest file describing the jobs and data requirements for the application", required=True)
    parser.add_argument("--localWorkingDir", help = "path to dir with read/write for processing data for upload and download to AWS S3", required=True)
    parser.add_argument("--documentName", help = "optional name of document to download (if unspecified all 'AWSToLocal' documents are downloaded)", required=False) 
    try:
        args = vars(parser.parse_args())
        manifestPath = os.path.abspath(args["manifestPath"])
        localWorkingDir = os.path.abspath(args["localWorkingDir"])

        s3 = boto3.resource('s3')

        app = Application(s3, manifestPath, localWorkingDir)

        if "documentName" in args and not args["documentName"] is None:
            app.downloadS3Document(args["documentName"]) 
        else:
            app.downloadS3Documents()

    except Exception as ex:
        logging.exception("error in downloader")
        sys.exit(1)

if __name__ == "__main__":
    main()


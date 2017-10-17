import boto3, argparse, os, sys
from instancemanager import InstanceManager
from application import Application
from loghelper import LogHelper
def main():
    LogHelper.start_logging("logdownloader.log")
    parser = argparse.ArgumentParser(
        description="AWS bootstrapper log downloader" +
                    "Downloads instances logs from AWS S3")
    parser.add_argument("--manifestPath", help = "path to a manifest file describing the jobs and data requirements for the application", required=True)

    try:
        args = vars(parser.parse_args())
        manifestPath = os.path.abspath(args["manifestPath"])
        outputdir = os.path.abspath(args["outputPath"])
        s3 = boto3.resource('s3')
        app = Application(s3, manifestPath, outputdir)
        app.downloadLogs(outputdir)

    except Exception as ex:
        logging.exception("error in downloader")
        sys.exit(1)

if __name__ == "__main__":
    main()

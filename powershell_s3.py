import subprocess, logging

class powershell_s3(object):
    """drop in replacement for boto3 s3 (windows only) which i cannot get working with userdata scripts..."""
    def Bucket(self, bucketName):
        self.bucketName = bucketName
        return self

    def download_file(self, keyName, localPath):
        cmd = ['powershell', 
               'Copy-S3Object', 
               '-BucketName', "'{}'".format(self.bucketName),
               '-LocalFile', "'{}'".format(localPath),
               '-Key', "'{}'".format(keyName)]
        self.execute(cmd)

    def upload_file(self, localPath, keyName):
        cmd = ['powershell', 
               'Write-S3Object', 
               '-BucketName', "'{}'".format(self.bucketName),
               '-File', "'{}'".format(localPath),
               '-Key', "'{}'".format(keyName)]
        self.execute(cmd)

    def execute(self, command):
        try:
            logging.info("issuing command: {0}".format(command))
            cmnd_output = subprocess.check_output(command, 
                                                    stderr=subprocess.STDOUT,
                                                    shell=False, 
                                                    universal_newlines=True);
            logging.info("command executed successfully")
        except subprocess.CalledProcessError as cp_ex:
            logging.exception("error occurred running command")
            logging.error(cp_ex.output)
            raise cp_ex
        except Exception as ex:
            logging.exception("error occurred running command")
            raise ex

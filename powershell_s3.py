import subprocess, logging, shutil, os

class powershell_s3(object):
    """drop in replacement for boto3 s3 (windows only)"""
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
        # the Write-S3Object command appears to require a file that has no other processes touching it
        # so make a copy
        tmpFile = "{}.tmp".format(localPath)
        shutil.copyfile(localPath, tmpFile)
        cmd = ['powershell', 
               'Write-S3Object', 
               '-BucketName', "'{}'".format(self.bucketName),
               '-File', "'{}'".format(tmpFile),
               '-Key', "'{}'".format(keyName)]
        self.execute(cmd)
        os.remove(tmpFile)

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

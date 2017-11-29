import logging
from time import sleep
from instancemanager import InstanceManager
class BlockingDownloader(object):

    def __init__(self, manifest, instance_manager, s3interface):
        self.sleepInterval = 60
        self._manifest = manifest
        self._instance_manager = instance_manager
        self.s3interface = s3interface
        self.allJobIds = [x["Id"] for x in self._manifest.GetJobs()]
        self.activeJobs = set(self.allJobIds)


    def run(self):
        while len(self.activeJobs) > 0:
            for x in self.allJobsIds:
                if not x in self.activeJobs:
                    continue
                instanceMeta = self._instance_manager.downloadMetaData(k)

                #check if all tasks are finished in the current instance
                if instanceMeta.AllTasksFinished():
                    logging.info("jobs on instance id {0} finished, downloading results".format(x))
                    job = self._manifest.GetJob(x)
                    jobDocuments = job["Documents"]
                    #download the instance's AWSToLocal documents
                    for docName in jobDocuments:
                        docMatches = self.manifest.GetS3Documents(
                            filter = 
                                {
                                    "Direction": "AWSToLocal",
                                    "Name": docName
                                })
                        for doc in docMatches:
                            self.s3interface.downloadCompressed(
                                self.manifest.GetS3KeyPrefix(), doc["Name"],
                                os.path.abspath(doc["LocalPath"]))
                    self.activeJobs.remove(x)

            sleep(self.sleepInterval)





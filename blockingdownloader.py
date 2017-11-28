from instancemanager import InstanceManager
class BlockingDownloader(object):

    def __init__(self, manifest, instance_manager, s3interface):
        self._manifest = manifest
        self._instance_manager = instance_manager
        self.s3interface = s3interface
        self.activeJobs = { x["Id"]: True for x in self._manifest.GetJobs() }

    def run(self):
        while len(self.activeJobs) > 0:
            for x in self.activeJobs.keys():
                # get instance metadata
                meta = self._instance_manager.downloadMetaData(k)

                #check if all tasks are finished in the current instance
                if meta.AllTasksFinished():
                    #set active to false
                    self.activeJobs[x] = False
                    for doc in self.manifest.GetS3Documents(filter = {"Direction": "AWSToLocal"}):
                        self.s3interface.downloadCompressed(self.manifest.GetS3KeyPrefix(), doc["Name"],
                                                            os.path.abspath(doc["LocalPath"]))




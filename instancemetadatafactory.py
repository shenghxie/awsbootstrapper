import json
from timehelper import TimeHelper
from instancemetadata import InstanceMetadata

class InstanceMetadataFactory(object):

    def __init__(self, manifest):
        self.manifest = manifest

    def InitializeMetadata(self, id, aws_id):
        ncommands = len(self.manifest.GetJob(id)["Commands"])
        nuploads = self._GetUploadCount(id)
        ndownloads = len(self.manifest.GetJob(id)["RequiredS3Data"]) - nuploads
        inst = InstanceMetadata(id=id,
                                aws_id=aws_id,
                                ncommands = ncommands,
                                nuploads = nuploads,
                                ndownloads = ndownloads,
                                lastmessage = "Initialize",
                                lastupdate = TimeHelper.GetUTCNowString(),
                                ncommandsFinished = 0,
                                nuploadsFinished = 0,
                                ndownloadsFinished = 0)
        return inst

    def _GetUploadCount(self, id):
        count = 0
        alldocs = { x["Name"] : x for x in self.manifest.GetS3Documents() }
        for r in self.manifest.GetJob(id)["RequiredS3Data"]:
            if alldocs[r]["Direction"] == "AWSToLocal":
                count = count + 1
        return count

    def ToJson(self, inst, outputpath):
        with open(outputpath, 'w') as outfile:
            json.dump(inst._doc, outfile, indent=4)

    def FromJson(self, filepath):
        with open(filepath) as f:
            data = json.load(f)
            inst = InstanceMetadata(
                data["Id"],
                data["AWS_Instance_Id"],
                data["CommandCount"],
                data["UploadCount"],
                data["DownloadCount"],
                data["LastMessage"],
                data["LastUpdate"],
                data["NCommandsFinished"],
                data["NUploadsFinished"],
                data["NDownloadsFinished"])
        return inst


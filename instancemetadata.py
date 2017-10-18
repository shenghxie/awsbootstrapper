import json
import datetime
from manifest import Manifest
class InstanceMetadata(object):

    def __init__(self, id, aws_id, ncommands, nuploads, ndownloads,
                lastmessage, lastupdate, ncommandsFinished, nuploadsFinished,
                ndownloadsFinished):
         self._doc = {
            "Id": id,
            "AWS_Instance_Id": aws_id,
            "CommandCount": ncommands,
            "UploadCount": nuploads,
            "DownloadCount": ndownloads,
            "LastMessage": lastmessage,
            "LastUpdate": lastupdate,
            "NCommandsFinished": ncommandsFinished,
            "NUploadsFinished": nuploadsFinished,
            "NDownloadsFinished": ndownloadsFinished
        }

    def Get(self, name):
        return self._doc[name]

    def AllTasksFinished(self):
        return self.Get("NCommandsFinished") == self.Get("CommandCount") and \
               self.Get("NUploadsFinished") == self.Get("UploadCount") and \
               self.Get("NDownloadsFinished") == self.Get("DownloadCount")

    def TotalTasks(self):
        return self.Get("CommandCount") + \
               self.Get("UploadCount") + \
               self.Get("DownloadCount")

    def UpdateMessage(self, msg):
        self._doc["LastUpdate"] = self.GetUTCNowString() 
        self._doc["LastMessage"] = msg

    def IncrementUploadsFinished(self):
        self._doc["NUploadsFinished"] += 1
        self._doc["LastMessage"] = "upload finished"
        self._doc["LastUpdate"] = self.GetUTCNowString() 

    def IncrementCommandFinished(self):
        self._doc["NCommandsFinished"] += 1
        self._doc["LastMessage"] = "command finished"
        self._doc["LastUpdate"] = self.GetUTCNowString()

    def IncrementDownloadFinished(self):
        self._doc["NDownloadsFinished"] += 1
        self._doc["LastMessage"] = "download finished"
        self._doc["LastUpdate"] = self.GetUTCNowString()

    @staticmethod
    def GetUTCNowString():
        return datetime.datetime.utcnow().strftime("%Y%m%d-%H:%M:%S UTC")
    
    @staticmethod
    def ParseUTCString(str):
        return datetime.datetime.strptime(str, "%Y%m%d-%H:%M:%S %Z")

    def GetTimeSinceLastUpdate(self):
        lastUpdateTime = self.ParseUTCString(self._doc["LastUpdate"])
        currentTime = datetime.datetime.utcnow()
        if lastUpdateTime > currentTime:
            return 0
        return (currentTime - lastUpdateTime).total_seconds()

    @staticmethod
    def __GetUploadCount(manifest):
        count = 0
        alldocs = { x["Name"] : x for x in manifest.GetS3Documents() }
        for r in manifest.GetJob(id)["RequiredS3Data"]:
            if alldocs[r]["Direction"] == "AWSToLocal":
                count = count + 1
        return count

    @staticmethod 
    def Initialize(id, aws_id, manifest):
        ncommands = len(manifest.GetJob(id)["Commands"])
        nuploads = InstanceMetadata.__GetUploadCount(manifest)
        ndownloads = len(manifest.GetJob(id)["RequiredS3Data"]) - nuploads
        inst = InstanceMetadata(id=id,
                                aws_id=aws_id,
                                ncommands = ncommands,
                                nuploads = nuploads,
                                ndownloads = ndownloads,
                                lastmessage = "Initialize",
                                lastupdate = InstanceMetadata.GetUTCNowString(),
                                ncommandsFinished = 0,
                                nuploadsFinished = 0,
                                ndownloadsFinished = 0)
        return inst

    @staticmethod
    def ToJson(inst, outputfile):
        with open(outputfile, 'w') as outfile:
            json.dump(inst._doc, outfile, indent=4)

    @staticmethod
    def FromJson(filepath):
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

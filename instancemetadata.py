from timehelper import TimeHelper
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
        self._doc["LastUpdate"] = TimeHelper.GetUTCNowString() 
        self._doc["LastMessage"] = msg

    def IncrementUploadsFinished(self):
        self._doc["NUploadsFinished"] += 1
        self._doc["LastMessage"] = "upload finished"
        self._doc["LastUpdate"] = TimeHelper.GetUTCNowString() 

    def IncrementCommandFinished(self):
        self._doc["NCommandsFinished"] += 1
        self._doc["LastMessage"] = "command finished"
        self._doc["LastUpdate"] = TimeHelper.GetUTCNowString()

    def IncrementDownloadFinished(self):
        self._doc["NDownloadsFinished"] += 1
        self._doc["LastMessage"] = "download finished"
        self._doc["LastUpdate"] = TimeHelper.GetUTCNowString()

    def GetTimeSinceLastUpdate(self):
        return TimeHelper.GetTimeElapsed(self._doc["LastUpdate"])



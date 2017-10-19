import datetime

class TimeHelper(object):

    @staticmethod
    def GetUTCNowString():
        return datetime.datetime.utcnow().strftime("%Y%m%d-%H:%M:%S UTC")
    
    @staticmethod
    def ParseUTCString(str):
        return datetime.datetime.strptime(str, "%Y%m%d-%H:%M:%S %Z")

    @staticmethod
    def GetTimeElapsed(t0_str):
        """
        Compute how much time has elapsed since a UTC datetime in the past
        @param t0_str string formatted UTC date time
        @returns the time in seconds elapsed since t0_str
        """
        t0 = TimeHelper.ParseUTCString(t0_str)
        t1 = datetime.datetime.utcnow()
        if t0 > t1:
            return 0
        return (t1 - t0).total_seconds()
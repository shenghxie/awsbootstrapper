import logging
class LogHelper(object):

    @staticmethod
    def start_logging(fn=".\\script.log",fmode='w'):
        #set up logging to print to console window and to log file
        #
        # From http://docs.python.org/2/howto/logging-cookbook.html#logging-cookbook
        #
        rootLogger = logging.getLogger()

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
            datefmt='%m-%d %H:%M')
        rootLogger.removeHandler(rootLogger.handlers[0]) #remove the basic handler
        logFormatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s', datefmt='%m-%d %H:%M')

        fileHandler = logging.FileHandler(fn, fmode)
        fileHandler.setFormatter(logFormatter)
        rootLogger.addHandler(fileHandler)

        consoleHandler = logging.StreamHandler()
        consoleHandler.setFormatter(logFormatter)
        rootLogger.addHandler(consoleHandler)

        rootLogger.setLevel(logging.INFO)
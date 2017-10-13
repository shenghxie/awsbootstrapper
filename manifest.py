import json, os, logging
from sets import Set

class Manifest(object):
    # class for processing the manifest information
    def __init__(self, manifestPath):
        self.validDirections = [
            "LocalToAWS", # document is copied to S3 by this application. Any document required by instance jobs will be downloaded by instance
            "AWSToLocal", # document is placed on S3 by an instance (eg. simulation output).  The document will be downloaded to a local path by this application.
            "Static" # document is assumed to be on S3 outside this scope of this application.  Can be downloaded to instances
        ]
        with open(manifestPath) as f:
            self.data = json.load(f)
        logging.info("validating manifest")
        self.__checkBasicFormat()
        s = self.__checkForDuplicateDocumentNames()
        self.__checkForDuplicateJobIds()
        self.__checkForMissingDocumentReferencedInJob(s)
        self.__checkAWSToLocalJobsReferences()

    def __checkBasicFormat(self):
        expectedKeys = set([
            "ProjectName",
            "BucketName",
            "Documents",
            "InstanceJobs" ])

        foundKeys = set(self.data.keys())
        if expectedKeys != foundKeys:
            raise ValueError("expected {0} sections in config, found {1}"
                .format(expectedKeys, foundKeys))

    def __checkAWSToLocalJobsReferences(self):
        '''
        If any 2 or more instances refer to the same AWSToLocal document, this
        is troublesome, since each of the instances will be writing, and or
        overwriting the same s3-key
        '''
        jobs = self.GetJobs()
        if len(jobs) == 0:
            return
        #collect AWSToLocal docs
        docs = set([x["Name"] for x in self.GetS3Documents(filter = {"Direction": "AWSToLocal"})])
        docset = set([])
        for j in jobs:
            jobdocs = j["RequiredS3Data"]
            #check if the same name appears 2 times in the same job, which is also an error
            if len(set(jobdocs)) != len(jobdocs):
                raise ValueError(
                    "duplicate required document name(s) detected in job {0}"
                    .format(j["Id"]))
            for docname in jobdocs:
                if docname not in docs:
                    continue # not an AWSToLocal job, multiple references are fine
                if docname in docset:
                    #the docname refers to an AWSToLocal document, and it was referenced in at least one other job
                    raise ValueError("AWSToLocal document '{0}' found in more than one job".format(docname))
                else:
                    docset.add(docname)



    def __checkForDuplicateDocumentNames(self):
        '''
        do not allow the same document name to appear twice in the manifest
        '''
        s = Set([])
        if "Documents" in self.data:
            for doc in self.data["Documents"]:
                name = doc["Name"]
                if name in s:
                    raise ValueError("duplicate document name detected in manifest '{0}'"
                                     .format(name))
                else:
                    s.add(name)
        return s

    def __checkForDuplicateJobIds(self):
        '''
        do not allow the same job id to appear twice in the manifest
        '''
        s = Set([])
        if "InstanceJobs" in self.data:
            for job in self.data["InstanceJobs"]:
                id = job["Id"]
                if id in s:
                    raise ValueError("duplicate job id detected in manifest '{0}'"
                                     .format(id))
                else:
                    s.add(id)

    def __checkForMissingDocumentReferencedInJob(self, nameset):
        if "InstanceJobs" in self.data:
            for job in self.data["InstanceJobs"]:
                documentList = job["RequiredS3Data"]
                for d in documentList:
                    if d not in nameset:
                        raise ValueError("Referenced S3 document '{0}' not found in defined documents"
                                         .format(d))

    def validateDirection(self, direction):
        if(direction in self.validDirections):
            return True
        else:
            return False

    def GetBucketName(self):
        return self.data["BucketName"]

    def GetS3Documents(self, filter=None):
        """get the sequence of S3 documents to process using the specified
        optional filters"""
        matches = []
        for doc in self.data["Documents"]:
            if not self.validateDirection(doc["Direction"]):
                raise ValueError("specified direction should be one of {0}"
                             .format(",".join(self.validDirections)))
            match = True
            if not filter is None:
                for k,v in filter.iteritems():
                    if doc[k] != v:
                        match = False
                        break

            if match:
                matches.append(doc)
        return matches

    def GetJob(self, instanceId):
        """get the job to run on the specified instance"""
        for job in self.data["InstanceJobs"]:
            if instanceId == job["Id"]:
                return job
        raise ValueError("specified job id {0} not found in manifest"
                         .format(instanceId))


    def GetS3KeyPrefix(self):
        """constructs the "directory" in S3 in which the documents are stored"""
        root = self.data["ProjectName"]
        return root

    def GetJobs(self):
        jobs = []
        for job in self.data["InstanceJobs"]:
            jobs.append(job)
        return jobs
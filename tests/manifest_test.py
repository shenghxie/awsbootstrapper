import unittest
import os, json
from manifest import Manifest
class Manifest_Test(unittest.TestCase):

    def tearDown(self):
        if os.path.exists(self.path):
            os.remove(self.path)

    def writeTestJsonFile(self, contents):
        self.path = os.path.join(os.getcwd(),"tests","test_manifest.json")
        with open(self.path, 'w') as f:
            f.write(json.dumps(contents))
        return os.path.abspath(self.path)

    def test_ValidateDirectionReturnsExpectedValue(self):
        
        m = Manifest(self.writeTestJsonFile({
                "BucketName": "myBucketName",
                "Documents":[]
            }))
        self.assertFalse( m.validateDirection("a"))
        self.assertTrue(m.validateDirection("LocalToAWS")) 
        self.assertTrue(m.validateDirection("AWSToLocal"))
        self.assertTrue(m.validateDirection("Static"))

    def test_GetBucketName(self):
        m = Manifest(self.writeTestJsonFile(
            {
                "BucketName": "myBucketName",
                "Documents":[]
            }))
        self.assertEqual( m.GetBucketName(), "myBucketName");
        
    def test_GetS3DocumentsThrowsErrorWithBadDirectionParameterInJson(self):
        
        m = Manifest(self.writeTestJsonFile({
        "ProjectName": "testProject",
        "BucketName": "bucket",
        "Documents": [
        {
        "Name": "document1",
        "Direction": "InvalidDirection",
        "LocalPath": ".",
        "AWSInstancePath": "awsinstancepath"
        },
        ]}))

        with self.assertRaises(ValueError) as context:
            m.GetS3Documents()

    def test_GetS3DocumentsThrowsErrorWithBadFilter(self):

        m = Manifest(self.writeTestJsonFile({
        "Documents": [
          {
            "Name": "document1",
            "Direction": "LocalToAWS",
            "LocalPath": ".",
            "AWSInstancePath": "awsinstancepath"
          },
        ]}))
        self.assertRaises(KeyError, 
                          lambda: list(m.GetS3Documents(filter={"nonmatchingkey": "value"})))

    def test_GetS3DocumentsReturnsExpectedValue(self):
        m = Manifest(self.writeTestJsonFile({
        "Documents": [
          {
            "Name": "document1",
            "Direction": "LocalToAWS",
            "LocalPath": ".",
            "AWSInstancePath": "awsinstancepath"
          },
          {
            "Name": "document2",
            "Direction": "AWSToLocal",
            "LocalPath": ".",
            "AWSInstancePath": "awsinstancepath1"
          },
          {
            "Name": "document3",
            "Direction": "LocalToAWS",
            "LocalPath": ".",
            "AWSInstancePath": "awsinstancepath2"
          },
        ]}))

        result = list(m.GetS3Documents())
        self.assertEqual(result[0]["Name"], "document1")
        self.assertEqual(result[0]["Direction"], "LocalToAWS")
        self.assertEqual(result[0]["LocalPath"], ".")
        self.assertEqual(result[0]["AWSInstancePath"], "awsinstancepath")
        
        self.assertEqual(result[1]["Name"], "document2")
        self.assertEqual(result[1]["Direction"], "AWSToLocal")
        self.assertEqual(result[1]["LocalPath"], ".")
        self.assertEqual(result[1]["AWSInstancePath"], "awsinstancepath1")

        self.assertEqual(result[2]["Name"], "document3")
        self.assertEqual(result[2]["Direction"], "LocalToAWS")
        self.assertEqual(result[2]["LocalPath"], ".")
        self.assertEqual(result[2]["AWSInstancePath"], "awsinstancepath2")

    def test_GetS3DocumentsReturnsFilteredDocuments(self):
        m = Manifest(self.writeTestJsonFile({
        "Documents": [
          {
            "Name": "document1",
            "Direction": "LocalToAWS",
            "LocalPath": ".",
            "AWSInstancePath": "awsinstancepath"
          },
          {
            "Name": "document2",
            "Direction": "AWSToLocal",
            "LocalPath": ".",
            "AWSInstancePath": "awsinstancepath"
          },
          {
            "Name": "document3",
            "Direction": "LocalToAWS",
            "LocalPath": ".",
            "AWSInstancePath": "awsinstancepath"
          },
        ]}))

        result = list(m.GetS3Documents(filter={"Direction": "LocalToAWS"}))
        self.assertEqual(result[0]["Name"], "document1")
        self.assertEqual(result[1]["Name"], "document3")

    def test_errorThrownOnDuplicateDocumentNames(self):
        self.assertRaises(ValueError, lambda: Manifest(self.writeTestJsonFile({
            "ProjectName": "testProject",
            "BucketName": "bucket",
            "Documents": [
              {
                "Name": "document",
                "Direction": "AWSToLocal",
                "LocalPath": "mylocalPath",
                "AWSInstancePath": "awsinstancepath"
              },
              {
                "Name": "document",
                "Direction": "LocalToAWS",
                "LocalPath": "mylocalPath",
                "AWSInstancePath": "awsinstancepath"
              },
            ]})))

    def test_errorThrownOnDuplicateJobIds(self):
        self.assertRaises(ValueError, lambda: Manifest(self.writeTestJsonFile({
            "ProjectName": "testProject",
            "BucketName": "bucket",
            "Documents": [
              {
                "Name": "document",
                "Direction": "AWSToLocal",
                "LocalPath": "mylocalPath",
                "AWSInstancePath": "awsinstancepath"
              },
            ],
            "InstanceJobs": [
            {
              "Id": 1,
              "RequiredS3Data": [ "document" ],
              "Commands": [{ "Command": "run.exe", "Args": [ ] } ]
            },
            {
              "Id": 1,
              "RequiredS3Data": [ "document" ],
              "Commands": [{ "Command": "run.exe", "Args": [ ] } ]
            }
            ]})))

    def test_errorThrownOnMissingRequiredS3DataInJob(self):
        self.assertRaises(ValueError, lambda: Manifest(self.writeTestJsonFile({
            "ProjectName": "testProject",
            "BucketName": "bucket",
            "Documents": [
              {
                "Name": "document",
                "Direction": "AWSToLocal",
                "LocalPath": "mylocalPath",
                "AWSInstancePath": "awsinstancepath"
              },
            ],
            "InstanceJobs": [
            {
              "Id": 1,
              "RequiredS3Data": [ "a_missing_document" ],
              "Commands": [{ "Command": "run.exe", "Args": [ ] } ]
            }
            ]})))

    def test_GetJobThrowsErrorOnMissingJobId(self):
        m = Manifest(self.writeTestJsonFile({
            "ProjectName": "testProject",
            "BucketName": "bucket",
            "Documents": [
              {
                "Name": "document",
                "Direction": "AWSToLocal",
                "LocalPath": "mylocalPath",
                "AWSInstancePath": "awsinstancepath"
              },
            ],
            "InstanceJobs": [
            {
              "Id": 1,
              "RequiredS3Data": [ "document" ],
              "Commands": [{ "Command": "run.exe", "Args": [ ] } ]
            },
            {
              "Id": 2,
              "RequiredS3Data": [ "document" ],
              "Commands": [{ "Command": "run.exe", "Args": [ ] } ]
            },
            {
              "Id": 3,
              "RequiredS3Data": [ "document" ],
              "Commands": [{ "Command": "run.exe", "Args": [ ] } ]
            }]}))
        self.assertRaises(ValueError, lambda: m.GetJob(999))

    def test_GetJobReturnsExpectedValues(self):
        m = Manifest(self.writeTestJsonFile({
            "ProjectName": "testProject",
            "BucketName": "bucket",
            "Documents": [
              {
                "Name": "document1",
                "Direction": "AWSToLocal",
                "LocalPath": ".",
                "AWSInstancePath": "awsinstancepath"
              },
              {
                "Name": "document2",
                "Direction": "LocalToAWS",
                "LocalPath": ".",
                "AWSInstancePath": "awsinstancepath"
              },
            ],
            "InstanceJobs": [
            {
              "Id": 1,
              "RequiredS3Data": [ "document1" ],
              "Commands": [{ "Command": "run.exe", "Args": [ 1, 2, 3] } ]
            },
            {
              "Id": 2,
              "RequiredS3Data": [ "document1" ],
              "Commands": [{ "Command": "run2.exe", "Args": [ 3, 2, 1] } ]
            },
            {
              "Id": 3,
              "RequiredS3Data": [ "document1", "document2" ],
              "Commands": [{ "Command": "run3.exe", "Args": [ 1] },
                           { "Command": "run4.exe", "Args": [ "a", "b"] }  ]
            }]}))

        j1 = m.GetJob(1)
        self.assertEqual(j1["Id"], 1)
        self.assertEqual(j1["RequiredS3Data"], [ "document1" ])
        self.assertEqual(j1["Commands"], [{ "Command": "run.exe", "Args": [ 1, 2, 3] } ])

        j2 = m.GetJob(2)
        self.assertEqual(j2["Id"], 2)
        self.assertEqual(j2["RequiredS3Data"], [ "document1" ])
        self.assertEqual(j2["Commands"], [{ "Command": "run2.exe", "Args": [3,2,1] } ])

        j3 = m.GetJob(3)
        self.assertEqual(j3["Id"], 3)
        self.assertEqual(j3["RequiredS3Data"], [ "document1", "document2" ])
        self.assertEqual(j3["Commands"],[{ "Command": "run3.exe", "Args": [ 1] },
                                         { "Command": "run4.exe", "Args": [ "a", "b"] }  ])

if __name__ == '__main__':
    unittest.main()

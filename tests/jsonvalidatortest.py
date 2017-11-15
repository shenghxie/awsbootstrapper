import unittest
from jsonvalidator import JsonValidator
from jsonschema import ValidationError
from jsonschema import SchemaError
class JsonValidatorTest(unittest.TestCase):
    def test_constructor(self):
        j = JsonValidator("schema", "json")
        self.assertEqual(j.schema, "schema")
        self.assertEqual(j.json, "json")

    def test_validate_returns_true(self):
        schema = {
            "type" : "object",
            "properties" : {
                "price" : {"type" : "number"},
                "name" : {"type" : "string"},
            },
        }
        json = {"name" : "Eggs", "price" : 34.99}
        j = JsonValidator(schema, json)
        self.assertTrue(j.Validate())

    def test_validate_raises_on_validation(self):
        
        schema = {
            "type" : "object",
            "properties" : {
                "price" : {"type" : "number"},
                "name" : {"type" : "string"},
            },
            "required": ["name"]
        }
        json = {"price" : 34.99}
        j = JsonValidator(schema, json)
        self.assertRaises(ValidationError, 
                          j.Validate)

    def test_validate_raises_on_schema(self):
        schema = {
            "type" : "object",
            "properties" : ["abc"]
        }
        json = {"price" : 34.99}
        j = JsonValidator(schema, json)
        self.assertRaises(SchemaError, 
                          j.Validate)

    def test_minimalManifestValidation(self):
        manifest = {
            "ProjectName": "AWSBootstrapperWindowsExample", 
            "BucketName": "my_bucket", 
            "Documents": [
                    {
                        "Direction": "LocalToAWS", 
                        "AWSInstancePath": "C:\\work\\input", 
                        "Name": "input", 
                        "LocalPath": ".\\input"
                    }, 
                    {
                        "Direction": "AWSToLocal", 
                        "AWSInstancePath": "C:\\work\\output", 
                        "Name": "output", 
                        "LocalPath": ".\\output"
                    }
            ],
            "InstanceJobs": [
                {
                    "Id": 1,
                    "Commands": [
                        {
                            "Args": [ "hello world", "c:\\work\\output\\helloworld.txt" ], 
                            "Command": "c:\\work\\input\\instanceJob.bat"
                        }
                    ], 
                    "RequiredS3Data": [
                        "input",
                        "output"
                    ]
               }
            ]
        }



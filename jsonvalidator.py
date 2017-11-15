from jsonschema import validate


class JsonValidator(object):
    def __init__(self, schema, json):
        self.schema = schema
        self.json = json

    def Validate(self):
        validate(self.json, self.schema)
        return True
 

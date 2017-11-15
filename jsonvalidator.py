import logging
from jsonschema import validate

class JsonValidator(object):
    def __init__(self, schema, json):
        self.schema = schema
        self.json = json

    def Validate(self):
        try:
            validate(json, schema)
        except ValidationError as ex:
            logging.info(ex.msg)
            return False
        return True


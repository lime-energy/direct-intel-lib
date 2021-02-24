import typing
from pandas_schema.validation_warning import ValidationWarning
class SchemaValidationError(Exception):

   def __init__(self, errors: typing.List[ValidationWarning], message="One or more validations failed"):
        self.errors = errors
        self.message = message
        super().__init__(self.message)
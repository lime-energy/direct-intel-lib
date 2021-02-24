import typing
from pandas_schema.validation_warning import ValidationWarning

class SchemaValidationError(Exception):
    def __init__(
        self, 
        warnings, 
        message="One or more validations failed"
    ):
        self.warnings = warnings
        self.message = message
        super().__init__(self.message)
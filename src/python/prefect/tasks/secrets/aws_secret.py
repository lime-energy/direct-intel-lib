import boto3
import json

from prefect.utilities.tasks import defaults_from_attrs
from prefect.tasks.secrets import SecretBase

from typing import Any, Optional


class AWSSecret(SecretBase):
    """
    AWS Secrets Task.  This task retrieves the underlying secret through
    the AWS Secrets Manager API.

    Args:
        - name (str, optional): The name of the underlying secret
        - key (str, optional): The key under the secret
        - **kwargs (Any, optional): additional keyword arguments to pass to the Task constructor

    Raises:
        - ValueError: if a `result` keyword is passed

    Examples:

    ```python
    secret = AWSSecret(name='prefect/secrets', key='db_url', default='Default')
    ```

    """

    def __init__(self, name=None, key=None, template=None, default:Optional[Any]=None, **kwargs):
        if key and template:
            raise ValueError("Can't use 'key' and 'template' at the same time.")

        self.secret_name = name
        self.key = key
        self.template = template
        self.default = default
        super().__init__(name=name, **kwargs)

    @defaults_from_attrs('secret_name', 'key', 'template')
    def run(self, secret_name: str = None, key: str = None, template: str = None):
        """
        The run method for Secret Tasks.  This method actually retrieves and returns the
        underlying secret value using the `Secret.get()` method.  Note that this method first
        checks context for the secret value, and if not found either raises an error or queries
        Custom Cloud, depending on whether `config.cloud.use_local_secrets` is `True` or
        `False`.

        Args:
            - secret_name (str, optional): the name of secret on AWS Secrets Manager
            - key (str, optional): the name of the key on your secret
            - template (str, optional): templated string that replaces the names with the values
              on the secret. Identifiers inside '{}' will be replaced, ie: 'constant {replaced}'.
              Dont use f strings

        Returns:
            - Any: the underlying value of the Custom Secret
        """
        if secret_name is None:
            raise ValueError("A secret name must be provided.")

        if key is None and template is None:
            raise ValueError("A key or template must be provided.")

        return self.get(secret_name, key, template)


    def get(self) -> Optional[str]:
        """
        Retrieve the secret value.  If not found, returns `None`.

        If using local secrets, `Secret.get()` will attempt to call `json.loads` on the
        value pulled from context.  For this reason it is recommended to store local secrets as
        JSON documents to avoid ambiguous behavior.

        Returns:
            - str: the value of the secret; if not found, raises an error

        Raises:
            - KeyError: if it fails to retrieve your secret
        """

        try:
            client = boto3.client('secretsmanager')
            resp = client.get_secret_value(SecretId=secret_name)
            secret = json.loads(resp['SecretString'])

            if template:
                return template.format(**secret)
            else:
                return secret[key]
        except:
            if self.default is not None:
                return self.default

            raise KeyError('Unable to fetch aws secret')

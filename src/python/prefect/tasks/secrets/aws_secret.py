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
    """

    def __init__(self, name=None, key=None, default:Optional[Any]=None, **kwargs):
        self.secret_name = name
        self.key = key
        self.default = default
        super().__init__(name=name, **kwargs)

    @defaults_from_attrs('secret_name', 'key')
    def run(self, secret_name: str = None, key: str = None):
        """
        The run method for Secret Tasks.  This method actually retrieves and returns the
        underlying secret value using the `Secret.get()` method.  Note that this method first
        checks context for the secret value, and if not found either raises an error or queries
        Custom Cloud, depending on whether `config.cloud.use_local_secrets` is `True` or
        `False`.

        Args:
            - name (str, optional): the name of the underlying Secret to retrieve. Defaults
                to the name provided at initialization.

        Returns:
            - Any: the underlying value of the Custom Secret
        """
        if secret_name is None:
            raise ValueError("A secret name must be provided.")

        if key is None:
            raise ValueError("A key must be provided.")

        return self.get()


    def get(self) -> Optional[Any]:
        """
        Retrieve the secret value.  If not found, returns `None`.

        If using local secrets, `Secret.get()` will attempt to call `json.loads` on the
        value pulled from context.  For this reason it is recommended to store local secrets as
        JSON documents to avoid ambiguous behavior.

        Returns:
            - Any: the value of the secret; if not found, raises an error

        Raises:
            - ValueError: if `.get()` is called within a Flow building context, or if
                `use_local_secrets=True` and your Secret doesn't exist
            - KeyError: if `use_local_secrets=False` and the Client fails to retrieve your secret
            - ClientError: if the client experiences an unexpected error communicating with the
                backend
        """

        if self.secret_name:
            try:
                client = boto3.client('secretsmanager')
                resp = client.get_secret_value(SecretId=self.secret_name)
                d = json.loads(resp['SecretString'])
                return d[self.key]
            except:
                if self.default is not None:
                    return self.default
                raise KeyError('Unable to fetch key')
        else:
            return self.default
        # if isinstance(prefect.context.get("flow"), prefect.core.flow.Flow):
        #     raise ValueError(
        #         "Secrets should only be retrieved during a Flow run, not while building a Flow."
        #     )

        # secrets = prefect.context.get("secrets", {})
        # try:
        #     value = secrets[self.name]
        # except KeyError:
        #     if prefect.config.backend != "cloud":
        #         raise ValueError(
        #             'Local Secret "{}" was not found.'.format(self.name)
        #         ) from None
        #     if prefect.context.config.cloud.use_local_secrets is False:
        #         try:
        #             result = self.client.graphql(
        #                 """
        #                 query($name: String!) {
        #                     secret_value(name: $name)
        #                 }
        #                 """,
        #                 variables=dict(name=self.name),
        #             )
        #         except ClientError as exc:
        #             if "No value found for the requested key" in str(exc):
        #                 raise KeyError(
        #                     f"The secret {self.name} was not found.  Please ensure that it "
        #                     f"was set correctly in your tenant: https://docs.prefect.io/"
        #                     f"orchestration/concepts/secrets.html"
        #                 ) from exc
        #             else:
        #                 raise exc
        #         # the result object is a Box, so we recursively restore builtin
        #         # dict/list classes
        #         result_dict = result.to_dict()
        #         value = result_dict["data"]["secret_value"]
        #     else:
        #         raise ValueError(
        #             'Local Secret "{}" was not found.'.format(self.name)
        #         ) from None
        # try:
        #     return json.loads(value)
        # except (json.JSONDecodeError, TypeError):
        #     return value
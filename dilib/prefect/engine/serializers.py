from typing import Any

from prefect.engine.serializers import Serializer


class NullSerializer(Serializer):
    """A Serializer does nothing"""

    def serialize(self, value: Any) -> bytes:
        return value

    def deserialize(self, value: bytes) -> Any:
        return value

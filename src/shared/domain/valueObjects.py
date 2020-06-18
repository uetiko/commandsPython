from uuid import uuid4
from uuid import UUID
from src.shared.domain.errors import UuidError


class Uuid(object):
    _value: uuid4 = None

    def __init__(self, value: str):
        self.validUuid(value)

    def validUuid(self, value: str):
        try:
            self._value = UUID(value, version=4)
        except ValueError as exception:
            raise UuidError(exception)

    @staticmethod
    def random() -> object:
        return Uuid(str(uuid4()))

    def toString(self) -> str:
        return str(self._value)

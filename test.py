from unittest import TestCase
from unittest import main
from src.shared.domain.valueObjects import Uuid
from src.shared.domain.errors import UuidError


class TestUuid(TestCase):

    def test_instance_of(self):
        self.assertIsInstance(
            Uuid.random(), Uuid
        )

    def test_to_string(self):
        uuid = Uuid.random()
        self.assertIsInstance(
            uuid.toString(), str
        )


class TestUuidExceptions(TestCase):

    def test_UuidError(self):
        with self.assertRaises(UuidError):
            Uuid('uuid')


if __name__ == '__main__':
    main()

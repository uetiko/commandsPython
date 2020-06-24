from abc import ABCMeta
from abc import abstractmethod


class FormalInterface(metaclass=ABCMeta):
    @classmethod
    def __subclasshook__(cls, subclass):
        return (
            hasattr(subclass, 'location') and
            callable(subclass.location) or
            NotImplemented
        )

    #@abstractmethod
    #def location(self, lon: float, lat: float) -> str:
    #    raise NotImplementedError


class SomeClass(FormalInterface):
    pass


some = SomeClass()
some.location(21.4, 34)

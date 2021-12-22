from gms.core.GmsBase import GmsBase


class Par(GmsBase):

    def __init__(self, name: str, dims: list):
        super().__init__(name, dims)
        pass

    def add_record(self, record: tuple):
        self._data.append(record)

    def __str__(self):
        return str(self._data)

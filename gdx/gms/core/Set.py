from gms.core.GmsBase import GmsBase


class Set(GmsBase):

    def __init__(self, name: str, dims: list):
        super().__init__(name, dims)

    def add_record(self, record_data: tuple):
        if len(record_data) != len(self.dims):
            raise ValueError()

        self._data.append(record_data)

    def is_element_in(self, value: str) -> bool:
        return tuple([value]) in self._data

    def __str__(self):
        return str(self._data)

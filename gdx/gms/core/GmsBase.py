class GmsBase:

    def __init__(self, name: str, dims: list):

        self.name = name
        self.dims = dims
        # TODO - прокинуть комментарии
        self.comment = ''
        self._data = []
        pass

    def get_data(self):
        return self._data.copy()

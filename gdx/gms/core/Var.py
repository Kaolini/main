from gms.core.GmsBase import GmsBase


class Var(GmsBase):

    def __init__(self, name, dims):
        super().__init__(self, name, dims)
        raise NotImplementedError('GamsVar is not implemented')

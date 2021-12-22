from gams import *

from gms.core.Par import Par
from gms.core.Set import Set
from gms.core.Var import Var


class GdxHelper:

    def __init__(self):
        self._sets = []
        self._pars = []
        self._vars = []
        self._cur_write_gdx_path = ''

    def begin_write(self, gdx_path: str):
        self._sets.clear()
        self._pars.clear()
        self._vars.clear()
        self._cur_write_gdx_path = gdx_path

    def add_set(self, s: Set):
        self._sets.append(s)

    def add_par(self, p: Par):
        self._pars.append(p)

    def commit_write(self):
        ws = GamsWorkspace()
        db = ws.add_database()

        for s in self._sets:
            gams_set = db.add_set(s.name, len(s.dims), s.comment)

            # TODO - перейти на итераторы
            for data_item in s.get_data():
                gams_set.add_record(data_item)

        for p in self._pars:
            # par_set_lst = []
            # for dim in p.dims:
            #     if isinstance(dim, Set):
            #         par_set_lst.append(db.get_set(dim.name))
            #     elif isinstance(dim, str):
            #         par_set_lst.append(dim)
            #     else:
            #         raise ValueError()

            gams_par = db.add_parameter(p.name, len(p.dims), p.comment)
            # TODO - перейти на итераторы
            for data_item in p.get_data():
                gams_par.add_record(data_item[0:len(p.dims)]).value = data_item[len(p.dims):][0]

        for v in self._vars:
            # TODO - имплементировать для vars как будет время
            pass

        db.export(self._cur_write_gdx_path)

        self._sets.clear()
        self._pars.clear()
        self._vars.clear()
        self._cur_write_gdx_path = ''


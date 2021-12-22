import os
import openpyxl
import  ntpath
import sys

from gms.core.Par import Par
from gms.core.Set import Set


class InputDataHelper:

    def __init__(self):
        self.xl_path = ''
        self.db_conn_str = ''
        self.query_dir = ''
        self.skip_empty_values = False
        self.skip_non_existent_values = False
        self.for_plan = False

    def set_xl_file_path(self, xl_file_path: str):
        self.xl_path = xl_file_path

    def set_db_connection_param(self, dialect: str, driver: str, user: str,
                                password: str, host: str, port: int, dbname: str):
        self.db_conn_str = f'{dialect}+{driver}'
        if driver == '':
            self.db_conn_str = f'{dialect}://{user}:{password}@{host}:{str(port)}/{dbname}'
        else:
            self.db_conn_str = f'{dialect}+{driver}://{user}:{password}@{host}:{str(port)}/{dbname}'

    def set_query_dir(self, path: str):
        self.query_dir = path

    # region Create Set Functionality
    def create_gmsset(self, mode: str, name: str, dims: list,
                      xl_range: str = '', query: str = '', query_id: str = '',
                      query_params: dict = None) -> Set:
        # TODO - сделать проверку всех параметров
        if mode == 'xl':
            return self._create_gmsset_xl(name, dims, xl_range)

        elif mode == 'db':
            if query == '':
                query_str = self._read_sql_query_file(query_id, query_params)
            else:
                query_str = query
            return self._create_gmsset_db(name, dims, query_str)

        else:
            raise ValueError(f'Mode {mode} is not supported')

    def _create_gmsset_xl(self, name: str, dims: list, xl_range: str):

        wb, sheet, sheet_name, sheet_range = self._prepare_xl(xl_range)

        data_lst = []
        for row_tpl in sheet[sheet_range]:
            value_tpl = [cell.value for cell in row_tpl]
            value_tpl = tuple(value_tpl)
            data_lst.append(value_tpl)
            for i in data_lst:
                if None in i:
                    data_lst.remove(i)
        return self._create_gmsset_from_list(name, dims, data_lst)

    def _create_gmsset_db(self, name: str, dims: list, query: str):
        from sqlalchemy import create_engine
        engine = create_engine(self.db_conn_str)

        with engine.connect() as conn:
            cursor = conn.execute(query)
            query_result = cursor.fetchall()
        if self.for_plan:
            data_lst = [tuple(res_legacy_row) for res_legacy_row in query_result]
            for res_legacy_row in query_result:
                if res_legacy_row in data_lst:
                    continue
                else:
                    data_lst.append(tuple(res_legacy_row))
        else:
            data_lst = [tuple(res_legacy_row) for res_legacy_row in query_result]
        return self._create_gmsset_from_list(name, dims, data_lst)

    def _create_gmsset_from_list(self, name: str, dims: list, data_lst: list):
        res = Set(name, dims)

        for rec_tpl in data_lst:
            if len(dims) != len(rec_tpl):
                raise ValueError(f'Dim count of set and record is different: {len(rec_tpl)} != {len(dims)}')

            for value_ix in range(len(rec_tpl)):
                dim = dims[value_ix]
                value = rec_tpl[value_ix]

                # Проверка на существование элемента в домене множества (если этот домен объявлен)
                if not self._check_element_existence_if_need(value, dim):
                    raise ValueError(f'Element {value} of parameter {name} not in domain set {dim.name}')

                # Проверка на пустое значение
                if not self._check_null_or_empty_if_need(value):
                    raise ValueError(f'Value {value} is empty! Creating par is {name}.')

            res.add_record(rec_tpl)

        return res

    # endregion

    # region Create Par Functionality
    def create_gmspar(self, mode: str, name: str, dims: list = None, rdims: list = None, cdims: list = None,
                      xl_range: str = '', query: str = '', query_id: str = '',
                      query_params: dict = None) -> Par:
        if mode == 'xl':
            return self._create_gmspar_xl(name, rdims, cdims, xl_range)
        elif mode == 'db':
            if query == '':
                query_str = self._read_sql_query_file(query_id, query_params)
            else:
                query_str = query
            return self._create_gmspar_db(name, dims, query_str)
        else:
            raise ValueError(f'Unsupported mode: {mode}')

    def _create_gmspar_xl(self, name: str, rdims: list, cdims: list, xl_range: str) -> Par:
        wb, sheet, sheet_name, sheet_range = self._prepare_xl(xl_range)

        # Заголовки столбцов
        col_header_lst = []
        col_header_row_ix = 0
        for row_tpl in sheet[sheet_range]:
            row_lst = list(row_tpl)
            col_header_lst.append([cell.value for cell in row_lst])

            col_header_row_ix += 1
            if len(cdims) <= col_header_row_ix:
                break

        sheet_data_lst = []
        for row_tpl in sheet[sheet_range]:
            row_lst = list(row_tpl)
            sheet_data_lst.append([cell.value for cell in row_lst])

        data_lst = []
        for row_lst in sheet_data_lst[len(cdims):]:
            # польностью пустые строки игнорируются
            if all(v is None for v in row_lst):
                continue

            cur_row_header = row_lst[0:len(rdims)]
            for col_ix in range(len(rdims), len(row_lst)):
                cur_col_header_lst = []
                for col_header in col_header_lst:
                    cur_col_header_lst.append(col_header[col_ix])
                cur_col_header = cur_col_header_lst

                if row_lst[col_ix] is None:
                    value = 0.
                else:
                    value = float(row_lst[col_ix])

                data_lst.append(tuple(cur_row_header + cur_col_header + [value]))

        return self._create_gmspar_from_list(name, rdims + cdims, data_lst)

    def _create_gmspar_db(self, name: str, dims: list, query: str) -> Par:
        from sqlalchemy import create_engine
        engine = create_engine(self.db_conn_str)

        with engine.connect() as conn:
            cursor = conn.execute(query)
            query_result = cursor.fetchall()

        query_result_lst = [list(res_legacy_row) for res_legacy_row in query_result]
        data_lst = []
        for data_item in query_result_lst:
            data_item[len(data_item)-1] = float(data_item[len(data_item)-1])
            data_lst.append(tuple(data_item))
        return self._create_gmspar_from_list(name, dims, data_lst)

    def _create_gmspar_from_list(self, name, dims: list, data_lst: list) -> Par:
        res = Par(name, dims)
        for record in data_lst:
            for dim_ix in range(len(record)-1):
                value = record[dim_ix]
                dim = dims[dim_ix]

                # Проверка на существование элемента в домене множества (если этот домен объявлен)
                if not self._check_element_existence_if_need(value, dim):
                    raise ValueError(f'Element {value} of parameter {name} not in domain set {dim.name}')

                # Проверка на пустое значение
                if not self._check_null_or_empty_if_need(value):
                    raise ValueError(f'Value {value} is empty! Creating par is {name}.')

            res.add_record(record)

        return res

    # endregion
    def _read_sql_query_file(self, query_id: str, query_params: dict) -> str:
        path = str(ntpath.dirname(sys.argv[0]))
        with open(path +"\\"+ str(self.query_dir + '\\' + query_id + '.sql')) as qf:
            query_str = qf.read()

        for key, value in query_params.items():
            query_str = query_str.replace(f'${key}', value)

        return query_str

    def _prepare_xl(self, xl_range: str) -> tuple:

        wb = openpyxl.load_workbook(self.xl_path)
        sheet_name = xl_range.split('!')[0]
        sheet_range = xl_range.split('!')[1]
        sheet = wb.get_sheet_by_name(sheet_name)

        return wb, sheet, sheet_name, sheet_range

    def _check_element_existence_if_need(self, element: str, g_set: Set) -> bool:
        if self.skip_non_existent_values:
            return True

        if isinstance(g_set, Set):
            return g_set.is_element_in(element)

        else:
            return True

    def _check_null_or_empty_if_need(self, element) -> bool:
        if self.skip_empty_values:
            return True
        elif element is None:
            return False
        elif element == '':
            return False
        else:
            return True

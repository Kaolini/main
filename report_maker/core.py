import psycopg2 as pg
from openpyxl import load_workbook
import pathlib
import datetime


class Core:

    def __init__(self, database, user, password,host, port):
        self.dir_path = str(pathlib.Path().resolve())
        self.list_col = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
                         'T', 'V', 'X', 'Y', 'Z', 'AA', 'AB', 'AC', 'AD', 'AE', 'AF', 'AG', 'AH', 'AI', 'AJ', 'AK']
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port


    def execute(self, req_name: list):
        connect = pg.connect(database=self.database,
                             user=self.user,
                             password=self.password,
                             host=self.host,
                             port=self.port)
        cursor = connect.cursor()
        data = []
        for req in req_name:
            cursor.execute(self.read_sql(req))
            res = cursor.fetchall()
            data.append(res)
        return data

    def read_sql(self, req_name):
        with open(str(self.dir_path + '/' + 'sql' + '/' + req_name + '.sql'), encoding="utf-8") as qf:
            query_str = qf.read()
        return query_str

    def get_xml_file(self, ex_file):
        wb = load_workbook(ex_file)
        return wb


    def clear_values(self, ex_file, sheets_to_values: dict):
        file = self.get_xml_file(ex_file)
        for sheet in sheets_to_values.keys():
            name_sheet = sheet
            sheet = file.get_sheet_by_name(sheet)
            for range in sheets_to_values[name_sheet]:
                for value in sheet[f'{range[0]}:{range[1]}']:
                    for cell in value:
                        cell.value = None
        return file

    def insert_data(self, ex_file, sheets_to_values: dict, data, row_count = 2):
        file = self.clear_values(ex_file, sheets_to_values)
        data = data
        for sheet in sheets_to_values.keys():
            sheet = file.get_sheet_by_name(sheet)
            row_count = row_count
            for row in data:
                for elem in row:
                    elem_count = 0
                    # if not type(elem) == tuple:
                    #     sheet[f'{self.list_col[elem_count]}{row_count}'].value = elem
                    #     elem_count += 1
                    if type(elem) == tuple:
                        for el in elem:
                            print(elem)
                            print(el)
                            sheet[f'{self.list_col[elem_count]}{row_count}'].value = el
                            elem_count += 1
                        row_count += 1
                row_count += 1
        return file

    def form_first_rep(self):
        ranges = {"Сводка": [['B4', 'B15'], ['C20', 'C30']],
                  "Проблемы": [['A2', 'N10000']],
                  'Исходные данные': [['A2', 'AG10000']]}
        ins = {"Проблемы": [['A2', 'N10000']]}
        clear = self.clear_values(r'C:\Users\mixaz\Desktop\Шаблоны\Отчет о загрузке от 07.12.2021.xlsx', ranges)
        data = self.execute(['1', '2', '3'])
        file = self.insert_data(r'C:\Users\mixaz\Desktop\Шаблоны\Отчет о загрузке от 07.12.2021.xlsx', ins,
                                data[2])
        return file

    def form_sec_rep(self):
        ranges = {'Лист1': [['A3', 'N10000']]}
        data = self.execute(['4'])
        file = self.insert_data(
            r'C:\Users\mixaz\Desktop\Шаблоны\Список повторно направленных пациентов от 07.12.2021.xlsx', ranges,
            data,row_count=3)
        return file

    def form_thi_rep(self):
        ranges = {'29.11.2021': [['A2', 'M10000']]}
        data = self.execute(['5'])
        data = [data]
        file = self.insert_data(
            r'C:\Users\mixaz\Desktop\Шаблоны\Список_вопросов_к_МО_по_новым_пациентам_за_07.12.2021.xlsx', ranges,
            data)
        return file

    def create_reports(self):
        fir = self.form_first_rep()
        sec = self.form_sec_rep()
        thi = self.form_thi_rep()

        fir.save('fist.xlsx')
        sec.save('second.xlsx')
        thi.save('third.xlsx')
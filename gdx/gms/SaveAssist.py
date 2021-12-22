import psycopg2 as pg
from gams import *
import ntpath
import sys
from collections import Counter
import os
from urllib.parse import urlparse
from colorama import init



class SaveAssist:

    def __init__(self, gdx_file: str, integro: int, dir_path:str, con_str: str):
        self.gdx_file = gdx_file
        self.integro = integro
        self.con_str = con_str
        self.dir_path = dir_path

    def get_gdx_data(self, set_name: str):
        ws = GamsWorkspace()
        db = ws.add_database_from_gdx(self.gdx_file)
        set = []
        set = [elem.keys for elem in db[set_name]]

        return set

    def process_sets(self):
        init()
        print('Processing...', end = '')
        inp = self.get_list_set()['gpt_cr_rep']
        g = self.get_list_set()['g']
        plan = self.get_list_set()['plan']
        cl = self.get_list_set()['cgpt_cr_rep']
        for k in inp:
            count = 1
            for i in k[1:]:
                k[count] = int(i.split('_')[1])
                count += 1
        fir = []
        for k in inp:
            fir.append(k[0])
        double = dict(Counter(fir))
        print('\r\033[KMakingRequests...')
        # for k in inp:
        #     if k[0] in double.keys():
        #         print(f'{inp.index(k)}/{len(inp)}', end='')
        #         k[0] = int(
        #             self.execute('sfg', {'unite_rep': str(k[0]), 'subj_rep': str(k[1]), 'integro_rep': self.integro})[
        #                 0][0])
        #         print('\r\033[K', end = '')
        print('First done!', end = '')
        for i in inp:
            i[-2] = int(self.execute('class_id', {'class_id': str(i[-2]), "integro_rep": self.integro})[0][0])
        print('\r\033[KSecond done!', end='')
        for i in inp:
            i.append(int(self.execute('calc_id', {'integro_rep': self.integro})[0][0]))
        print('\r\033[KThird done!', end='')
        for i in inp:
            i.append(int(self.execute('day_numb', {'integro_rep': self.integro, "ord_rep": str(i[-2])})[0][0]))
        print('\r\033[KForth done!', end='')
        for i in inp:
            i[-3] = int(self.execute('day_numb', {'integro_rep': self.integro, 'ord_rep': str(i[-3])})[0][1])
        print('\r\033[KFifth done!')
        count = 0
        for i in inp:
            i.append(cl[count][0])
        res = []
        for i in inp:
            res.append(tuple(i))
        for i in res:
            if '' in i:
                raise ValueError(f'Row {i} has empty values')
        return res

    def get_list_set(self):
        gpt_cr_rep = self.get_gdx_data('gpt_cr_rep')
        g = self.get_gdx_data('g')
        plan = self.get_gdx_data('plan')
        cgpt_cr_rep = self.get_gdx_data('cgpt_cr_rep')
        per = tuple(locals().keys())[1:]
        val = tuple(locals().values())[1:-1]
        result = {}
        k = 0
        for i in per:
            result[i] = val[k]
            k += 1
        return result

    def connect_to_db(self):
        con_str_conn = pg.connect(self.con_str)

        return con_str_conn

    def execute(self, set_name, values: dict = '', read_type='select', check_req=False):
        conn = self.connect_to_db()
        cursor = conn.cursor()
        res = self.read_sql(set_name, values, read_type)
        if read_type == 'select' or read_type == 'custom':
            if check_req:
                print(res)
            cursor.execute(res)
            res = cursor.fetchall()
            return res
        else:
            if check_req:
                print(res)
            cursor.execute(res)
            conn.commit()
            cursor.close()
            conn.close()

    def read_sql(self, set_name, values = '', read_type: str = 'select'):
        path = ntpath.dirname(self.dir_path)
        with open(str(path + '/' + 'insert_sql' + '/' + set_name + '.sql')) as qf:
            query_str = qf.read()
        if read_type == 'insert':
            requests = self.process_sets()
            for i in requests:
                if not i == requests[-1]:
                    query_str = query_str.replace('$coll', str(i) + ',' + '\n' + '$coll')
                else:
                    query_str = query_str.replace('$coll', str(i))
        elif read_type == 'select':
            for i in values.keys():
                query_str = query_str.replace(i, values[i])
        else:
            query_str = query_str.replace('$coll', values)
        return query_str

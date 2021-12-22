import sys

from gms.InputDataHelper import InputDataHelper

PG_DIALECT = 'postgresql'
PG_DRIVER = ''
PG_USER = 'schedule_root'
PG_PASSWORD = 'Ckj;ysqgfh0km'
PG_HOST = '192.168.50.40'
PG_PORT = 5432
PG_DATABASE = 'schedule_simulation'


def main(argv):
    xl_path = 'xl_test.xlsx'
    gl = InputDataHelper()
    gl.set_xl_file_path(xl_path)
    gl.set_db_connection_param(PG_DIALECT, PG_DRIVER, PG_USER,
                               PG_PASSWORD, PG_HOST, PG_PORT, PG_DATABASE)
    gl.set_query_dir('test_queries')

    # Чтение простого множества
    g_set = gl.create_gmsset(mode='xl', name='g', dims=['group'], xl_range='sets!a3:a6')
    res_list = ['g1', 'g2', 'g3', 'g4']
    res = all([tpl[0] in res_list for tpl in g_set.get_data()])
    assert res is True, 'Test did not pass'

    # Чтение двойного множества с контролем измерения
    cg_rel = gl.create_gmsset(mode='xl', name='gpt_match', dims=['c', g_set], xl_range='sets!c3:d6')
    res_list = [('c1', 'g1'), ('c1', 'g2'), ('c2', 'g3'), ('c2', 'g4')]
    res = all([tpl in res_list for tpl in cg_rel.get_data()])
    assert res is True, 'Test did not pass'

    g_ = gl.create_gmsset('db', 'test', ['g'], query_id='test_set', query_params={'test_par': '1'})

    # tp = gl.create_gmspar('xl', 'test', rdims=['g'], cdims=['p'], xl_range='pars!a1:c5')
    # tp_1 = gl.create_gmspar('xl', 'test_1', rdims=['c', 'g'], cdims=['t', 'p'], xl_range='pars!f1:i6')
    tp_ = gl.create_gmspar('db', 'test_db', ['g'], query_id='test_par', query_params={'test_par': '1'})


if __name__ == '__main__':
    main(sys.argv)

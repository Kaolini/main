from gms.SaveAssist import SaveAssist
import pandas as pd

class Ranking(SaveAssist):



    def __init__(self, gdx_file: str, integro: int, dir_path,  con_str: str):
        self.gdx_file = gdx_file
        self.integro = integro
        self.con_str = con_str
        self.dir_path = dir_path



    def get_rank(self):
        data = super().process_sets()
        pr = [x[1] for x in data]
        # print(str(tuple(set(pr))))
        ranks = super().execute('ranks', str(tuple(set(pr))), read_type = 'custom')
        calc_info = super().execute('calc_info', {'integro_rep': self.integro})
        
        ranks_comp = {}
        data_upd = []
        for i in ranks:
            ranks_comp[i[0]] = i[1]
        for row in data:
            row = list(row)
            for key in ranks_comp.keys():
                if row[1] == key:
                    row.append(ranks_comp[key])
                    row = tuple(row)
                    data_upd.append(row)
        data_df = []
        for i in data_upd:
            data_df.append((i[-2], i[1], i[-1]))

        ds =  pd.DataFrame(data_df)
        ds.to_csv('test.csv')

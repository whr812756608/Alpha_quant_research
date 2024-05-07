"""
Path manager
"""
import os

DATA_DIR ='./'


def get_path(tbl_name, sub_dir=None, data_dir=DATA_DIR):
    """Mapping from tbl_name to file path
    :param tbl_name:
    :param data_dir: base data dir
    :param sub_dir: user provided specific directory under the mapped dir
    :return:
    """
    if not os.path.exists(data_dir):
        raise FileNotFoundError(f'{data_dir} does not exist')

    mapping = {'raw': {'1min': os.path.join(data_dir, 'raw', 'qishi_1min_zip'),
                       'adj_fct': os.path.join(data_dir, 'raw', 'other', 'adj_fct'),
                       'halt_date': os.path.join(data_dir, 'raw', 'other', 'date'),
                       'lst_date': os.path.join(data_dir, 'raw', 'other', 'date'),
                       'st_date': os.path.join(data_dir, 'raw', 'other', 'date'),
                       'trd_date': os.path.join(data_dir, 'raw', 'other', 'date'),
                       'idx': os.path.join(data_dir, 'raw', 'other', 'idx'),
                       'lmt': os.path.join(data_dir, 'raw', 'other', 'lmt'),
                       'mkt_val': os.path.join(data_dir, 'raw', 'other', 'mkt_val'),
                       'sw': os.path.join(data_dir, 'raw', 'other', 'sw'),
                       'hs300': os.path.join(data_dir, 'raw', 'other', 'univ', 'hs300'),
                       'zz500': os.path.join(data_dir, 'raw', 'other', 'univ', 'zz500'),
                       'zz800': os.path.join(data_dir, 'raw', 'other', 'univ', 'zz800'),
                       'zz1000': os.path.join(data_dir, 'raw', 'other', 'univ', 'zz1000'),
                       'zz9999': os.path.join(data_dir, 'raw', 'other', 'univ', 'zz9999')},
               'pkl': {'1min_interim': os.path.join(data_dir, 'pkl', '1min', 'interim'),
                       '1min_skw': os.path.join(data_dir, 'pkl', '1min'),
                       '1min_astd': os.path.join(data_dir, 'pkl', '1min'),
                       '1min_agg': os.path.join(data_dir, 'pkl', '1min'),
                       'adj_fct': os.path.join(data_dir, 'pkl', 'adj_fct'),
                       'halt_date': os.path.join(data_dir, 'pkl', 'date'),
                       'lst_date': os.path.join(data_dir, 'pkl', 'date'),
                       'st_date': os.path.join(data_dir, 'pkl', 'date'),
                       'trd_date': os.path.join(data_dir, 'pkl', 'date'),
                       'idx': os.path.join(data_dir, 'pkl', 'idx'),
                       'lmt': os.path.join(data_dir, 'pkl', 'lmt'),
                       'mkt_val': os.path.join(data_dir, 'pkl', 'mkt_val'),
                       'sw': os.path.join(data_dir, 'pkl', 'sw'),
                       'hs300': os.path.join(data_dir, 'pkl', 'univ'),
                       'zz500': os.path.join(data_dir, 'pkl', 'univ'),
                       'zz800': os.path.join(data_dir, 'pkl', 'univ'),
                       'zz1000': os.path.join(data_dir, 'pkl', 'univ'),
                       'zz9999': os.path.join(data_dir, 'pkl', 'univ'),
                       'alpha': os.path.join(data_dir, 'pkl', 'alpha')},
               'plot': {'alpha': os.path.join(data_dir, 'plot', 'alpha')}}
    tbl, tag = tbl_name.split('.')
    path = mapping[tag][tbl]
    if sub_dir is not None:
        path = os.path.join(path, sub_dir)
    if not os.path.exists(path):
        os.makedirs(path)
    return path

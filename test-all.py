import unittest
import ooc_wrapper
from sklearn import linear_model
import data_access
import sys

class SQLiteAccessTest(unittest.TestCase):
    def test_read(self):
        print('Running SQLite reader test...')
        db_source_dict = {'db_path':'/home/louis/Code/out-of-core-scikit/test_data',
                          'db_name': 'testdb',
                          'table': 'test_small'}
        acc = data_access.SQLiteAccess(batch_size=2000)
        i_o_dict=acc.read(db_source_dict, inputs=['A','B'] ,outputs=['D'])
        i,o = i_o_dict['inputs'], i_o_dict['outputs']
        print('Size of input iterator is ',format(sys.getsizeof(i)))
        assert [list(el) for el in list(i)[0]] == [[1, 0], [2, 0], [3, 0], [4, 0]]
        assert [list(element) for element in o] == [[1, 2, 3, 4]]
        i_o_dict=acc.read(db_source_dict, inputs=['A','B'])
        i = i_o_dict['inputs']
        assert [list(el) for el in list(i)[0]] == [[1, 0], [2, 0], [3, 0], [4, 0]]

    def test_end_to_end(self):
        print('Running end to end testing with SQLite datasource...')
        wrap=ooc_wrapper.OOCWrapper(model_type=linear_model.SGDRegressor, input_fields=['A','B'], output_field='D')
        training_source_dict = {'db_path':'/home/louis/Code/out-of-core-scikit/test_data',
                          'db_name': 'testdb',
                          'table': 'test_small',
                          'pk_column':'ID'}
        test_data_dict = {'db_path':'/home/louis/Code/out-of-core-scikit/test_data',
                          'db_name': 'testdb',
                          'table': 'test_results',
                          'pk_column':'ID',
                          'label':'TEST',
                          'label_column':'TIMESTAMP'}
        wrap.fit('sqlite', training_source_dict, iterations=10000)
        wrap.predict('sqlite', training_source_dict, 'sqlite', test_data_dict)

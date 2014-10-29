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
        i,o=acc.read(db_source_dict, ['A','B'] ,'D')
        print('Size of input iterator is ',format(sys.getsizeof(i)))
        assert [list(el) for el in list(i)[0]] == [[1, 0], [2, 0], [3, 0], [4, 0]]
        assert [list(element) for element in o] == [[1, 2, 3, 4]] # This test fails; not sure at what level to do the unpacking
        i=acc.read(db_source_dict, ['A','B'])
        assert [list(el) for el in list(i)[0]] == [[1, 0], [2, 0], [3, 0], [4, 0]]

    def test_end_to_end(self):
        print('Running end to end testing with CSV datasource...')
        wrap=ooc_wrapper.OOCWrapper(model_type=linear_model.SGDRegressor, input_fields=['A','B'], output_field='D')
        wrap.fit('csv', 'test_data/test.csv', iterations=10000)
        print('Expecting [1, 2]; [3, 4]')
        wrap.predict('test', None, 'test', None)

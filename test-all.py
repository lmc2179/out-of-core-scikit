import unittest
import ooc_wrapper
from sklearn import linear_model
import data_access
import sys

class DataStreamTest(unittest.TestCase):
    def test_data_stream(self):
        data_iterator = ([j for j in range(i)] for i in range(1,5))
        stream = data_access.DataStream(2, data_iterator)
        batches = stream.batches()
        assert [batch for batch in batches] == [  [[0],      [0, 1]],
                                                [ [0, 1, 2], [0, 1, 2, 3]]]

# TODO: This set of tests is antiquated; they will be removed after the system is reimplemented with datastream objects
class SQLiteAccessTest(unittest.TestCase):
    @unittest.skip('Planned for decommission')
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

    @unittest.skip('Planned for decommission')
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

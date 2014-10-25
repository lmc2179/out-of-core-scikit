import unittest
import ooc_wrapper
from sklearn import linear_model
import data_access

class OOCWrapperTest(unittest.TestCase):
    def test_wrapper_end_to_end(self):
        print('Running wrapper test...')
        wrap=ooc_wrapper.OOCWrapper(model_type=linear_model.SGDRegressor, input_fields=['A','B'], output_field='D')
        wrap.fit(None, 'test', iterations=10000)
        print('Expecting [1, 2]; [3, 4]')
        wrap.predict(None, 'test', None, 'test')

class CSVAccessTest(unittest.TestCase):
    def test_read(self):
        print('Running CSV reader test...')
        test_file = 'test_data/test.csv'
        acc = data_access.CSVAccess(batch_size=2000)
        i,o=acc.read(test_file, ['A','B'] ,'D')
        print(list(i))
        print(list(o))
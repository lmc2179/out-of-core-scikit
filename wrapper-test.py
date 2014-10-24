import unittest
import ooc_wrapper
from sklearn import linear_model


class OOCWrapperTest(unittest.TestCase):
    def test_wrapper_end_to_end(self):
        wrap=ooc_wrapper.OOCWrapper(model_type=linear_model.SGDRegressor, input_fields=['A','B'], output_field='D')
        wrap.fit(None, 'test', iterations=10000)
        print 'Expecting [1, 2]; [3, 4]'
        wrap.predict(None, 'test', None, 'test')
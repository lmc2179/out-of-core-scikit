import unittest
import ooc_wrapper
from sklearn import linear_model


class OOCWrapperTest(unittest.TestCase):
    def test_wrapper_end_to_end(self):
        wrap=ooc_wrapper.OOCWrapper(model_type=linear_model.SGDRegressor, input_fields=['A','B'], output_field='D')
        wrap.fit(None, 'test')
        wrap.predict(None, 'test', None, 'test')
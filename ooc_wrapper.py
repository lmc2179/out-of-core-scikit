import itertools
import data_access

class OOCWrapper(object):
    def __init__(self, model_type=None, input_fields = None, output_field=None):
        self.model = model_type()
        self.input_fields = input_fields
        self.output_field = output_field
        self.data_access_map = {'sqlite': data_access.SQLiteAccess}

    def fit(self, data_type, data_source_dict, iterations=1):
        for i in range(iterations):
            input_batches, output_batches = self._get_training_batches(data_source_dict, data_type, self.input_fields, self.output_field)
            [self.model.partial_fit(self._unpack_input_batch(i),self._unpack_output_batch(o)) for i,o in zip(input_batches, output_batches)]

    def _unpack_input_batch(self, batch):
        batch = [list(el) for el in list(batch)]
        return batch

    def _unpack_output_batch(self, batch):
        batch = [element for element in batch]
        return batch

    def _get_training_batches(self, data_source, data_type, input_fields, output_field):
        accessor = self.data_access_map[data_type]()
        io_dict = accessor.read(data_source, inputs=input_fields, output=output_field)
        return io_dict['inputs'], io_dict['output']

    def predict(self, data_type, data_source_dict, target_type, target_data_dict):
        input_batches = self._get_test_batches(data_source_dict, data_type, self.input_fields)
        output_batches = self._predict_batches(input_batches)
        self._write_predictions(target_data_dict, target_type, output_batches)

    def _get_test_batches(self, data_source, data_type, input_fields):
        accessor = self.data_access_map[data_type]()
        return accessor.read(data_source, inputs=input_fields)['inputs']

    def _predict_batches(self, input_batches):
        return (self.model.predict(list(i)) for i in input_batches)

    def _write_predictions(self, target_file, target_type, output_batches):
        accessor = self.data_access_map[target_type]()
        accessor.write(output_batches, target_file)

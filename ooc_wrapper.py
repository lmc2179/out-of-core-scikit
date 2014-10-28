import itertools
import data_access

class OOCWrapper(object):
    def __init__(self, model_type=None, input_fields = None, output_field=None):
        self.model = model_type()
        self.input_fields = input_fields
        self.output_field = output_field
        self.data_access_map = {'test':data_access.TestDataAccess, 'csv':data_access.CSVAccess}

    def fit(self, data_type, data_source_dict, iterations=1):
        for i in range(iterations):
            input_batches, output_batches = self._get_batches(data_source_dict, data_type, self.input_fields, self.output_field)
            [self.model.partial_fit(self._unpack_input_batch(i),self._unpack_output_batch(o)) for i,o in zip(input_batches, output_batches)]

    def _unpack_input_batch(self, batch):
        batch = [list(el) for el in list(batch)]
        return batch

    def _unpack_output_batch(self, batch):
        batch = [element for element in batch]
        return batch

    def _get_batches(self, data_source, data_type, input_fields, output_field=None):
        accessor = self.data_access_map[data_type]()
        batches = accessor.read(data_source, input_fields, output_field=output_field)
        return batches

    def predict(self, data_type, data_source_dict, target_type, target_data_dict):
        input_batches = self._get_batches(data_source_dict, data_type, self.input_fields)
        output_batches = self._predict_batches(input_batches)
        self._write_predictions(target_data_dict, target_type, output_batches)

    def _predict_batches(self, input_batches):
        return (self.model.predict(i) for i in input_batches)

    def _write_predictions(self, target_file, target_type, output_batches):
        accessor = self.data_access_map[target_type]()
        accessor.write(output_batches, target_file)

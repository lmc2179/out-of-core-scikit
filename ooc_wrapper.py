import itertools
import data_access

class OOCWrapper(object):
    def __init__(self, model_type=None, input_fields = None, output_field=None):
        self.model = model_type()
        self.input_fields = input_fields
        self.output_field = output_field
        self.data_access_map = {'sqlite': data_access.SQLiteAccess}

    def _validate(self, data_source_dict, required_fields):
        missing_fields = [field for field in required_fields if field not in data_source_dict]
        if not missing_fields:
            return
        raise Exception('Missing fields from data dictionary: {0}'.format(', '.join(missing_fields)))

    def fit(self, data_type, data_source_dict, iterations=1):
        self._validate(data_source_dict, data_access.FIELDS_REQUIRED_FOR_TRAINING_READ)
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
        self._validate(data_source_dict, data_access.FIELDS_REQUIRED_FOR_TESTING_READ)
        self._validate(target_data_dict, data_access.FIELDS_REQUIRED_FOR_TESTING_WRITE)
        pk_batches, input_batches = self._get_test_batches(data_source_dict, data_type, self.input_fields)
        output_batches = self._predict_batches(input_batches)
        self._write_predictions(target_data_dict, target_type, output_batches, pk_batches)

    def _get_test_batches(self, data_source, data_type, input_fields):
        accessor = self.data_access_map[data_type]()
        pk_field = data_source[data_access.PK_COLUMN]
        data_dict = accessor.read(data_source, inputs=input_fields, primary_key=[pk_field])
        return data_dict['primary_key'], data_dict['inputs']

    def _predict_batches(self, input_batches):
        return (self.model.predict(list(i)) for i in input_batches)

    def _write_predictions(self, target_file, target_type, output_batches, primary_keys):
        accessor = self.data_access_map[target_type]()
        accessor.write(primary_keys, output_batches, target_file)

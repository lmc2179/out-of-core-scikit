import itertools

class OOCWrapper(object):
    def __init__(self, model_type=None, input_fields = None, output_field=None):
        self.model_type = model_type
        self.input_fields = input_fields
        self.output_field = output_field
        self.reader_map = {}
        self.writer_map = {}

    def fit(self, data_source, data_type):
        input_batches, output_batches = self._get_batches(data_source, data_type, self.input_fields, self.output_field)
        [self.model_type.partial_fit(i,o) for i,o in itertools.izip(input_batches, output_batches)]

    def _get_batches(self, data_source, data_type, input_fields, output_field=None):
        reader = self.reader_map[data_type]
        batches = reader(data_source, input_fields, output_field=output_field)
        return batches

    def predict(self, data_source, data_type, target_file, target_type):
        input_batches = self._get_batches(data_source, data_type, self.input_fields)
        output_batches = self._predict_batches(input_batches)
        self._write_predictions(target_file, target_type, output_batches)

    def _predict_batches(self, input_batches):
        return (self.model_type.predict(i) for i in input_batches)

    def _write_predictions(self, target_file, target_type, output_batches):
        writer = self.writer_map[target_type]
        writer(output_batches, target_file)

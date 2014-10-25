import itertools
import csv

class AbstractDataAccess(object):
    def read(self, data_source, input_fields, output_field=None):
        raise NotImplementedError

    def write(self, output_batches, target_file):
        raise NotImplementedError

class TestDataAccess(object):
    def read(self, data_source, input_fields, output_field=None):
        test_data = {'A':[1,2,3,4], 'B':[0,0,0,0], 'C':[7,7,7,7], 'D':[1,2,3,4]}
        all_inputs = list(zip(*[test_data[f] for f in input_fields]))
        input_batches = all_inputs[:2], all_inputs[2:]
        if output_field is None:
            return input_batches
        all_outputs = [o for o in test_data[output_field]]
        output_batches = all_outputs[:2], all_outputs[2:]
        return input_batches, output_batches

    def write(self, output_batches, target_file):
        for o in output_batches:
            print(o)

class CSVAccess(AbstractDataAccess):
    def __init__(self, batch_size):
        self.batch_size = batch_size

    def read(self, filename, input_fields, output_field=None):
        f = csv.DictReader(open(filename))
        row_iter = (row for row in f)
        input_iter, output_iter = self._extract_vectors(row_iter, input_fields, output_field)
        input_batches = self._make_batches_from_iter(input_iter, self.batch_size)
        if not output_field:
            return input_batches
        output_batches = self._make_batches_from_iter(output_iter, self.batch_size)
        return input_batches, output_batches

    def _extract_vectors(self, row_iter, input_fields, output_field):
        input_iter = ([row[f] for f in input_fields] for row in row_iter)
        if not output_field:
            return input_iter
        output_iter = (row[output_field] for row in row_iter)
        return input_iter, output_iter

    def _make_batches_from_iter(self, iterator, batch_size):
        buffer = []
        try:
            while True:
                if(len(buffer) > batch_size):
                    yield buffer
                    buffer = []
                else:
                    buffer.append(next(iterator))
        except StopIteration:
            yield buffer
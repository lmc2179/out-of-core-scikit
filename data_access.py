import itertools

class AbstractDataAccess(object):
    def read(self, data_source, input_fields, output_field):
        raise NotImplementedError

    def write(self, output_batches, target_file):
        raise NotImplementedError

class TestDataAccess(object):
    def read(self, data_source, input_fields, output_field):
        test_data = {'A':[1,2,3,4], 'B':[0,0,0,0], 'C':[7,7,7,7], 'D':[1,2,3,4]}
        all_inputs = zip(*[test_data[f] for f in input_fields])
        input_batches = all_inputs[:2], all_inputs[2:]
        if output_field is None:
            return input_batches
        all_outputs = [o for o in test_data[output_field]]
        output_batches = all_outputs[:2], all_outputs[2:]
        return input_batches, output_batches

    def write(self, output_batches, target_file):
        for o in output_batches:
            print o
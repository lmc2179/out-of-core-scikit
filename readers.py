import functools
import itertools

def test_reader(data_source, input_fields, output_field):
    print 'Reading test data; data source ignored'
    test_data = {'A':[1,2,3,4], 'B':[0,0,0,0], 'C':[7,7,7,7], 'D':[1,2,3,4]}
    input_batches = itertools.izip(*[test_data[f] for f in input_fields])
    output_batches = (o for o in test_data['D'])
    return input_batches, output_batches

reader_map = {'test': functools.partial(test_reader)}
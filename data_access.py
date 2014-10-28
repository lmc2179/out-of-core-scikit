import csv
import sqlite3
import os

class AbstractDataAccess(object):
    def read(self, data_source_dict, input_fields, output_field=None):
        raise NotImplementedError

    def write(self, output_batches, target_dict):
        raise NotImplementedError

# TODO: These two classes are scaffolding; they will be removed when the SQLite code is ready.
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
    def __init__(self, batch_size=2000):
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
        if not output_field:
            input_iter = ([row[f] for f in input_fields] for row in row_iter)
            return input_iter, None
        combined_iter = (([row[f] for f in input_fields],row[output_field]) for row in row_iter) # Something strange happens here
        input_data, output_data = zip(*combined_iter) # Unzip operation;
        return iter(input_data), iter(output_data)

    def _make_batches_from_iter(self, iterator, batch_size):
        count = 0
        buffer = []
        batches = []
        for element in iterator:
            if count < batch_size:
                count += 1
                buffer.append(element)
            else:
                count = 0
                batches.append(iter(buffer))
                buffer = []
        batches.append(iter(buffer))
        return batches

class SQLiteAccess(AbstractDataAccess):
    def __init__(self, batch_size=2000):
        self.batch_size = batch_size
        self.DB_PATH = 'db_path'
        self.DB_NAME = 'db_name'
        self.TABLE_NAME = 'table'

    def read(self, data_source_dict, input_fields, output_field=None):
        "Takes location of the form [DB_NAME].[TABLE_NAME] and returns iterator over batches."
        self._validate(data_source_dict)
        if not output_field:
            return self._get_batch_stream(data_source_dict, input_fields)
        return self._get_batch_stream(data_source_dict, input_fields), self._get_batch_stream(data_source_dict, [output_field])

    def _validate(self, data_source_dict):
        missing_fields = [field for field in [self.DB_PATH, self.DB_NAME, self.TABLE_NAME] if field not in data_source_dict]
        if not missing_fields:
            return
        raise Exception('Missing fields from data source: {0}'.format(', '.join(missing_fields)))

    def _get_batch_stream(self, data_source_dict, fieldnames):
        full_db_path = os.path.join(data_source_dict[self.DB_PATH], data_source_dict[self.DB_NAME])
        sql = self._build_sql_select(data_source_dict[self.TABLE_NAME], fieldnames)
        with sqlite3.connect(full_db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            results = cursor.fetchall()[1:] # TODO: Add correct batching
            return results

    def _build_sql_select(self, table_name, fields):
        query = 'SELECT {0} FROM {1};'.format(','.join(fields), table_name)
        return query
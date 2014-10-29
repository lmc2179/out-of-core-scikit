import csv
import sqlite3
import os

class AbstractDataAccess(object):
    def read(self, data_source_dict, input_fields, output_field=None):
        raise NotImplementedError

    def write(self, output_batches, target_dict):
        raise NotImplementedError

    def _make_batches_from_iter(self, iterator, batch_size):
        # Utility method - this comes in handy pretty often.
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
            column_names = next(cursor)
            if len(fieldnames) > 1:
                return self._make_batches_from_iter(cursor, self.batch_size)
            else: # If there is only a single row, we unwrap it
                return self._make_batches_from_iter((record[0] for record in cursor), self.batch_size)

    def _build_sql_select(self, table_name, fields):
        query = 'SELECT {0} FROM {1};'.format(','.join(fields), table_name)
        return query
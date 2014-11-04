import itertools
import sqlite3
import os

#### Constants used by this module and ooc-wrapper
DB_PATH = 'db_path'
DB_NAME = 'db_name'
TABLE_NAME = 'table'
PK_COLUMN = 'pk_column'
FIELDS_REQUIRED_FOR_TRAINING_READ = [DB_PATH, DB_NAME, TABLE_NAME]
FIELDS_REQUIRED_FOR_TESTING_READ = [DB_PATH, DB_NAME, TABLE_NAME]
FIELDS_REQUIRED_FOR_TESTING_WRITE = [DB_PATH, DB_NAME, TABLE_NAME]

class AbstractDataAccess(object):
    def read(self, data_source_dict, **kwargs):
        raise NotImplementedError

    def write(self, primary_keys, output_batches, target_dict):
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

    def read(self, data_source_dict, **kwargs):
        "Takes location of the form [DB_NAME].[TABLE_NAME] and returns iterator over batches."
        self._validate(data_source_dict)
        return self._get_batch_streams(data_source_dict, **kwargs)

    def _validate(self, data_source_dict):
        missing_fields = [field for field in [DB_PATH, DB_NAME, TABLE_NAME] if field not in data_source_dict]
        if not missing_fields:
            return
        raise Exception('Missing fields from data source: {0}'.format(', '.join(missing_fields)))

    def _get_batch_stream(self, data_source_dict, fieldnames):
        full_db_path = os.path.join(data_source_dict[DB_PATH], data_source_dict[DB_NAME])
        sql = self._build_sql_select(data_source_dict[TABLE_NAME], fieldnames)
        with sqlite3.connect(full_db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            column_names = next(cursor)
            if len(fieldnames) > 1:
                return self._make_batches_from_iter(cursor, self.batch_size)
            else: # If there is only a single row, we unwrap it
                return self._make_batches_from_iter((record[0] for record in cursor), self.batch_size)

    def _get_batch_streams(self, data_source_dict, **kwargs):
        full_db_path = os.path.join(data_source_dict[DB_PATH], data_source_dict[DB_NAME])
        fieldnames = self._flatten_list(kwargs.values())
        sql = self._build_sql_select(data_source_dict[TABLE_NAME], fieldnames)
        # I apologize deeply for the following block of code.
        with sqlite3.connect(full_db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            column_names = fieldnames
            stream_names = kwargs.keys()
            stream_columns = kwargs.values()
            extractor_functions = self._build_extractors(column_names, stream_names, stream_columns) # Same ordering as stream_*
            stream_iterators = self._split_cursor(cursor, extractor_functions)
            batch_stream_iterators = [self._make_batches_from_iter(i, self.batch_size) for i in stream_iterators]
            streams_dict = dict(zip(stream_names, batch_stream_iterators))
        return streams_dict

    def _flatten_list(self, L):
        final_list = []
        for item in L:
            if isinstance(item, list):
                final_list += item
            else:
                final_list.append(item)
        return list(set(final_list)) # Extract unique elements

    def _build_extractors(self, query_column_order, stream_names, stream_columns):
        extractors = []
        column_lookup = dict((c,i) for i,c in enumerate(query_column_order)) # Map column name to position in the row
        for stream_name, stream_columns in zip(stream_names, stream_columns):
            extractors.append(self._build_extractor_function(column_lookup, stream_columns))
        return extractors

    def _build_extractor_function(self, column_lookup, stream_columns):
        def extractor(row):
            if len(stream_columns) == 1:
                col = stream_columns[0] # There is only one column that we need to extract from this stream
                return row[column_lookup[col]]
            else:
                return [row[column_lookup[col]] for col in stream_columns]
        return extractor

    def _split_cursor(self, cursor, extractor_functions):
        tees = itertools.tee(cursor, len(extractor_functions))
        return [map(f,t) for t,f in zip(tees, extractor_functions)]

    def _build_sql_select(self, table_name, fields):
        query = 'SELECT {0} FROM {1};'.format(','.join(fields), table_name)
        return query

    def write(self, primary_keys, output_batches, target_dict):
        self._validate(target_dict)
        [self._write_batch_stream(primary_keys, batch) for primary_keys, batch in zip(primary_keys,output_batches)]

    def _write_batch_stream(self, pk_batch, batch):
        print(list(zip(pk_batch, batch)))
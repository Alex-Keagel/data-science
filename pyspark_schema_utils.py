from numpy import unicode
import pyspark.sql.types as pst
from pyspark.sql import Row

def infer_schema(record):
    """infers dataframe schema for a record. Assumes every dict is a Struct, not a Map"""
    if isinstance(record, dict):
        # sorted(record.items() -> (key, value) needed to be an instance of list of tuples and not dict_items.
        # It needs to be sorted so the inferred struct will be const per record type
        return pst.StructType([pst.StructField(key, infer_schema(value), True)
                               for key, value in sorted(record.items())])
    elif isinstance(record, list):
        if len(record) == 0:
            raise ValueError("can't infer type of an empty list")
        element_type = infer_schema(record[0])
        for element in record:
            this_type = infer_schema(element)
            if element_type != this_type:
                raise ValueError("can't infer type of a list with inconsistent element types")
        return pst.ArrayType(element_type)
    else:
        return pst._infer_type(record)


def equivalent_types_checker(x, y):
    if type(x) in [str, unicode] and type(y) in [str, unicode]:
        return True
    return isinstance(x, type(y)) or isinstance(y, type(x))


def create_row_based_on_schema(row, record_example):
    """creates a Row object conforming to a schema as specified by a dict"""
    if row is None:
        return None
    elif isinstance(record_example, dict):
        if type(row) != dict:
            raise ValueError("expected dict, got {} instead -> {} <> {}".format(type(row), record_example, row))
        rowlike_dictionary = {}
        for key, val in record_example.items():
            if key not in record_example:
                raise ValueError("got unexpected field {}\n row = {}\nrecord_example = {}".format(key, row, record_example))
            rowlike_dictionary[key] = create_row_based_on_schema(row.get(key, None), record_example[key])
        return Row(**rowlike_dictionary)
    elif isinstance(record_example, list):
        if type(row) != list:
            raise ValueError("expected list, got {} instead".format(type(row)))
        return [create_row_based_on_schema(e, record_example[0]) for e in row]
    else:
        if not equivalent_types_checker(row, record_example):
            raise ValueError("expected {}, got {} instead -> {} <> {}".format(type(record_example), type(row), record_example, row))
        return row


def rdd_to_df(rdd, record_example, sql_context):
    """creates a dataframe out of an rdd of dicts, with schema inferred from a record_example record"""
    schema = infer_schema(record_example)
    row_rdd = rdd.map(lambda row: create_row_based_on_schema(row, record_example))
    return sql_context.createDataFrame(row_rdd, schema)

from pyflink.dataset import ExecutionEnvironment
from pyflink.table import TableConfig, DataTypes, BatchTableEnvironment, CsvTableSink, WriteMode, CsvTableSource
from pyflink.table.descriptors import Schema, OldCsv, FileSystem

exec_env = ExecutionEnvironment.get_execution_environment()
exec_env.set_parallelism(1)
t_config = TableConfig()
t_env = BatchTableEnvironment.create(exec_env, t_config)

# define the field names and types
field_names = ["word"]
field_types = [DataTypes.STRING()]

# create a TableSource
csv_source = CsvTableSource("tmp/input.csv", field_names, field_types)

# Register a TableSource
t_env.register_table_source("mySource", csv_source)

# create a TableSink
field_names = ["word", "count"]
field_types = [DataTypes.STRING(), DataTypes.BIGINT()]
csv_sink = CsvTableSink(field_names, field_types, "tmp/output/csv_output.csv", ",", 1, WriteMode.OVERWRITE)
t_env.register_table_sink("mySink", csv_sink)

"""
if use execute_insert,the below exception will be thrown out:
py4j.protocol.Py4JJavaError: An error occurred while calling o2.execute.
: org.apache.flink.optimizer.CompilerException: Bug: The optimizer plan represen
tation has no sinks.
"""

t_env.from_path('mySource').group_by('word').select('word, count(1)').insert_into('mySink')

t_env.execute("tutorial_job")

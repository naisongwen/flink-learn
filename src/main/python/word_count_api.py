from pyflink.dataset import ExecutionEnvironment
from pyflink.table import TableConfig, DataTypes, BatchTableEnvironment
from pyflink.table.descriptors import Schema, OldCsv, FileSystem

exec_env = ExecutionEnvironment.get_execution_environment()
exec_env.set_parallelism(1)
t_config = TableConfig()
t_env = BatchTableEnvironment.create(exec_env, t_config)

t_env.connect(FileSystem().path('tmp/input.csv')) \
    .with_format(OldCsv()
                 .field('word', DataTypes.STRING())) \
    .with_schema(Schema()
                 .field('word', DataTypes.STRING())) \
    .create_temporary_table('mySource')

t_env.connect(FileSystem().path('tmp/wc_api_output.csv')) \
    .with_format(OldCsv()
                 .field_delimiter('\t')
                 .field('word', DataTypes.STRING())
                 .field('count', DataTypes.BIGINT())) \
    .with_schema(Schema()
                 .field('word', DataTypes.STRING())
                 .field('count', DataTypes.BIGINT())) \
    .create_temporary_table('mySink')

t_env.from_path('mySource').group_by('word').select('word, count(1)').insert_into('mySink')
"""
if use execute_insert,the below exception will be thrown out:
py4j.protocol.Py4JJavaError: An error occurred while calling o49.executeInsert.
: java.lang.IllegalArgumentException: requirement failed: INSERT OVERWRITE requi
res OverwritableTableSink but actually got org.apache.flink.table.sinks.CsvTable
Sink
"""
# t_env.from_path('mySource').group_by('word').select('word, count(1)').execute_insert('mySink',True)

t_env.execute("tutorial_job")

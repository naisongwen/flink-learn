from pyflink.datastream  import StreamExecutionEnvironment
from pyflink.table import TableConfig, StreamTableEnvironment

exec_env = StreamExecutionEnvironment.get_execution_environment()
exec_env.set_parallelism(1)
t_config = TableConfig()
t_env = StreamTableEnvironment.create(exec_env, t_config)

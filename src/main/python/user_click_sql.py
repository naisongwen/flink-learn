from pyflink.datastream  import StreamExecutionEnvironment
from pyflink.table import TableConfig, StreamTableEnvironment

exec_env = StreamExecutionEnvironment.get_execution_environment()
exec_env.set_parallelism(1)
t_config = TableConfig()
t_env = StreamTableEnvironment.create(exec_env, t_config)

my_source_ddl = """
    create table clicks (
         `user` VARCHAR,
        `ctime` TIMESTAMP,
        `url` VARCHAR
    ) with (
        'connector' = 'filesystem',
        'format' = 'csv',
        'path' = 'tmp/click_input'
    )
"""

my_sink_ddl = """
    create table sink (
         `user` VARCHAR,
        `ctime` TIMESTAMP,
        `cnt` bigint
    ) with (
        'connector' = 'filesystem',
        'format' = 'csv',
        'path' = 'tmp/click_output'
    )
"""

my_sink_ddl = """
    create table sink (
         `user` VARCHAR,
        `ctime` TIMESTAMP,
        `cnt` bigint
    ) with (
        'connector' = 'print'
    )
"""
t_env.execute_sql(my_source_ddl)
t_env.execute_sql(my_sink_ddl)

#pyflink.util.exceptions.TableException: "AppendStreamTableSink doesn't support consuming
#update changes which is produced by node GroupAggregate(groupBy=[user, end_time],
#select=[user, end_time, COUNT(url) AS cnt])"

t_env.execute_sql("""
insert into sink select user,tumble_end(ctime,interval '1' hours) as end_time,count(url) as cnt
from clicks
group by user,tumble_end(ctime,interval '1' hours)
""")

t_env.execute("tutorial_job")

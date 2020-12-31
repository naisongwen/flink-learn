from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import TableConfig, StreamTableEnvironment

exec_env = StreamExecutionEnvironment.get_execution_environment()
exec_env.set_parallelism(1)
t_config = TableConfig()
t_env = StreamTableEnvironment.create(exec_env, t_config)

my_source_ddl = """
    create table mySource (
        word VARCHAR
    ) with (
        'connector' = 'filesystem',
        'format' = 'csv',
        'path' = 'tmp/input.csv'
    )
"""

my_sink_ddl = """
    create table mySink (
        word VARCHAR,
        `count` BIGINT
    ) with (
        'connector' = 'filesystem',
        'format' = 'csv',
        'path' = 'tmp/output/sql_output'
    )
"""

# org.apache.flink.table.api.ValidationException: Flink doesn't support ENFORCED mode for PRIMARY KEY constaint. ENFORCED/NOT ENFORCED  controls if the constraint checks are performed on the incoming/outgoing data.
#Flink does not own the data therefore the only supported mode is the NOT ENFORCED mode
mysql_sink_ddl = """
    create table mysql_sink (
        word VARCHAR,
        `count` BIGINT,
        primary key(word) NOT ENFORCED
    ) with (
        'connector.type' = 'jdbc',
        'url' = 'jdbc:mysql://192.168.1.18:3306/hzgas_hand',
        'table-name' = 'mysqlSink',
        'driver' = 'com.mysql.jdbc.Driver',
        'username' = 'hzgas',
        'password' = 'Hzgas@2019'
    )
"""

t_env.execute_sql(my_source_ddl)
t_env.execute_sql(my_sink_ddl)
t_env.execute_sql(mysql_sink_ddl)

"""
pyflink.util.exceptions.TableException: "AppendStreamTableSink doesn't support c
onsuming update changes which is produced by node GroupAggregate(groupBy=[word],
 select=[word, COUNT($f1) AS EXPR$0])"
"""
#t_env.from_path('mySource').group_by('word').select('word, count(1)').execute_insert("mySink", True)
#t_env.sql_query("select word,count(1) as cnt from mySource group by word").select('word,cnt').execute_insert("mySink", True)
#t_env.from_path('mySource').group_by('word').select('word, count(1)').insert_into('mySink')
t_env.from_path('mySource').select('word, 1').insert_into('mysql_sink')

t_env.execute("tutorial_job")

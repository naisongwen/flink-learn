package org.learn.flink.connector.jdbc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

public class TestFlinkJdbc {

    @Test
    public void testMysql() throws ClassNotFoundException {
        Class.forName("com.mysql.jdbc.Driver");
        String createTableSql="-- register a MySQL table 'users' in Flink SQL\n" +
                "CREATE TABLE %s (\n" +
                "  id BIGINT,\n" +
                "  name STRING,\n" +
                "  age INT,\n" +
                "  status BOOLEAN,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://10.201.0.214:3306/test',\n" +
                "   'table-name' = '%s',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '123456'\n" +
                ");";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        for(int i=0;i<1;i++) {
            String tableName="test_flink_jdbc_tbl_"+i;
            tEnv.executeSql(String.format(createTableSql,tableName,tableName));
            tEnv.executeSql("insert into "+tableName+" values(1,'xxx',18,true)");
        }
    }
}

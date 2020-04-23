package org.learn.flink.connector.hive;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

import static junit.framework.TestCase.assertEquals;

@RunWith(StandaloneHiveRunner.class)
public class HelloHiveRunner {

    @HiveSQL(files = {})
    private HiveShell shell;


    @Before
    public void setupSourceDatabase() {
        shell.execute("CREATE DATABASE source_db");
        shell.execute(new StringBuilder()
                .append("CREATE TABLE source_db.test_table (")
                .append("year STRING, value INT")
                .append(")")
                .toString());
    }

    @Test
    public void testMaxValueByYear() {
        /*
         * Insert some source data
         */
        shell.insertInto("source_db", "test_table")
                .withColumns("year", "value")
                .addRow("2014", 3)
                .addRow("2014", 4)
                .addRow("2015", 2)
                .addRow("2015", 5)
                .commit();

        /*
         * Execute the query
         */
//        shell.execute(Paths.get("src/test/resources/helloHiveRunner/calculate_max.sql"));

        /*
         * Verify the result
         */
        List<Object[]> result = shell.executeStatement("select * from source_db.test_table");
        assertEquals(4, result.size());
    }
}

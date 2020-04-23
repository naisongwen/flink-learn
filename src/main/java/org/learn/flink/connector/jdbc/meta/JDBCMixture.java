package org.learn.flink.connector.jdbc.meta;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBCMixture {
    public static final String INPUT_TABLE = "books";
    static final String OUTPUT_TABLE = "newbooks";

    public static void initSchema(DbMetadata dbMetadata) throws ClassNotFoundException, SQLException {
        System.setProperty("derby.stream.error.field", JDBCMixture.class.getCanonicalName() + ".DEV_NULL");
        Class.forName(dbMetadata.getDriverClass());
        try (Connection conn = DriverManager.getConnection(dbMetadata.getInitUrl(),dbMetadata.getLoginAccount(),dbMetadata.getLoginPwd())) {
            createTable(conn, INPUT_TABLE);
            createTable(conn, OUTPUT_TABLE);
        }
    }

    private static void createTable(Connection conn, String tableName) throws SQLException {
        Statement stat = conn.createStatement();
        stat.executeUpdate(getCreateQuery(tableName));
        stat.close();
    }

    private static String getCreateQuery(String tableName) {
        return "CREATE TABLE " + tableName + " (" +
                "id INT NOT NULL DEFAULT 0," +
                "title VARCHAR(50) DEFAULT NULL," +
                "author VARCHAR(50) DEFAULT NULL," +
                "price FLOAT DEFAULT NULL," +
                "qty INT DEFAULT NULL," +
                "PRIMARY KEY (id))";
    }
}

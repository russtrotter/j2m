package net.flatball.j2m.tool;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JDBCExtractor {
    private String jdbcDriver;
    private String jdbcUrl;
    private String jdbcUser;
    private String jdbcPassword;
    private Connection connection;
    private DatabaseMetaData databaseMetaData;
    private JDBCConsumer consumer;
    private static final String[] TABLES_ONLY = { "TABLE" };

    public void process() {
        try {
            Class.forName(jdbcDriver);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }

        try {
            if (jdbcUser != null && jdbcPassword != null) {
                connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
            } else {
                connection = DriverManager.getConnection(jdbcUrl);
            }

            consumer.databaseBegin(connection.getCatalog());
            databaseMetaData = connection.getMetaData();
            try {
                processConnection();
            } finally {
                connection.close();
            }
            consumer.databaseEnd();
        } catch (SQLException se) {
            throw new RuntimeException(se);
        }
    }

    private void processConnection() throws SQLException {
        List<String> tables = getTables();
        for (String table : tables) {
            consumer.tableBegin(table);
            processTable(table);
            consumer.tableEnd(table);
        }
    }

    private void processTable(String table) throws SQLException {
        Statement statement = connection.createStatement();
        try {
            String q = databaseMetaData.getIdentifierQuoteString();
            ResultSet rs = statement.executeQuery("SELECT * FROM " + q + table + q);
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();
            while (rs.next()) {
                consumer.rowBegin();
                for (int col = 1; col <= columnCount; col++) {
                    consumer.column(meta.getColumnName(col), rs.getObject(col));
                }
                consumer.rowEnd();
            }
        } finally {
            statement.close();
        }
    }

    private List<String> getTables() throws SQLException {
        List<String> tables = new ArrayList<String>();
        ResultSet rs = databaseMetaData.getTables(null, null, null, TABLES_ONLY);
        while (rs.next()) {
            String tableName = rs.getString(3);
            tables.add(tableName);
        }
        return tables;
    }

    public void setConsumer(JDBCConsumer consumer) {
        if (consumer == null) {
            throw new NullPointerException("Consumer must not be null");
        }
        this.consumer = consumer;
    }

    public void setJdbcDriver(String jdbcDriver) {
        this.jdbcDriver = jdbcDriver;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public void setJdbcUser(String jdbcUser) {
        this.jdbcUser = jdbcUser;
    }

    public void setJdbcPassword(String jdbcPassword) {
        this.jdbcPassword = jdbcPassword;
    }
}

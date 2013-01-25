package net.flatball.j2m.tool;

public class Main {
    public static void main(String[] args) {
        MongoDBJDBCConsumer consumer = new MongoDBJDBCConsumer();
        String mongoHost = System.getProperty("mongoHost");
        if (mongoHost != null) {
            consumer.setHost(mongoHost);
        }
        String mongoPort = System.getProperty("mongoPort");
        if (mongoPort != null) {
            consumer.setPort(Integer.parseInt(mongoPort));
        }
        String mongoDatabase = System.getProperty("mongoDatabase");
        if (mongoDatabase != null) {
            consumer.setDatabaseName(mongoDatabase);
        }



        consumer.initialize();



        String jdbcDriver = System.getProperty("jdbcDriver");
        if (jdbcDriver == null) {
            throw new IllegalArgumentException("Missing driverClass property");
        }
        String jdbcUrl = System.getProperty("jdbcUrl");
        if (jdbcUrl == null) {
            throw new IllegalArgumentException("Missing jdbcUrl property");
        }

        JDBCExtractor extractor = new JDBCExtractor();
        extractor.setConsumer(consumer);
        extractor.setJdbcDriver(jdbcDriver);
        extractor.setJdbcUrl(jdbcUrl);
        extractor.setJdbcUser(System.getProperty("jdbcUser"));
        extractor.setJdbcPassword(System.getProperty("jdbcPassword"));
        extractor.process();
    }
}

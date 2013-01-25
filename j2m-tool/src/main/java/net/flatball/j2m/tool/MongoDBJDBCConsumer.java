package net.flatball.j2m.tool;

import com.mongodb.*;

public class MongoDBJDBCConsumer implements JDBCConsumer {
    private String host = "localhost";
    private int port = 27017;
    private String databaseName;
    private MongoClient client;
    private DB db;
    private DBCollection collection;
    private BasicDBObjectBuilder builder;

    public void initialize() {
        try {
            client = new MongoClient(host, port);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public void databaseBegin(String name) {
        if (databaseName != null) {
            name = databaseName;
        }
        db = client.getDB(name);
    }

    public void databaseEnd() {
    }

    public void tableBegin(String table) {
        collection = db.getCollection(table);
    }

    public void tableEnd(String table) {
    }

    public void rowBegin() {
        builder = new BasicDBObjectBuilder();
    }

    public void rowEnd() {
        DBObject obj = builder.get();
        WriteResult result = collection.save(obj);
        CommandResult lastError = result.getLastError();
        if (lastError != null) {
            lastError.throwOnError();
        }
    }

    public void column(String name, Object value) {
        builder.add(name, value);
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }
}

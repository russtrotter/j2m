package net.flatball.j2m.tool;

public interface JDBCConsumer {
    void databaseBegin(String name);
    void databaseEnd();
    void tableBegin(String table);
    void tableEnd(String table);
    void rowBegin();
    void rowEnd();
    void column(String name, Object value);
}

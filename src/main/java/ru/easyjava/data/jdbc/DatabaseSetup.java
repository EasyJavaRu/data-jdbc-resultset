package ru.easyjava.data.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Creates database tables and initializes them with data.
 */
public final class DatabaseSetup {
    /**
     * Number of items in order.
     */
    private static final int MAX_ITEMS = 5;

    /**
     * We only ha3 clients.
     */
    public static final int MAX_CLIENTS = 3;

    /**
     * Query that creates order table.
     */
    private static final String CREATE_ITEM =
            "CREATE TABLE ORDER_ITEMS ("
                    + "ID IDENTITY PRIMARY KEY, "
                    + "CLIENT_ID BIGINT NOT NULL, "
                    + "ORDER_ID INTEGER NOT NULL, "
                    + "ITEM_ID INTEGER NOT NULL"
                    + ")";

    /**
     * Query that creates clients table.
     */
    private static final String CREATE_CLIENT =
            "CREATE TABLE CLIENTS ("
                    + "ID IDENTITY PRIMARY KEY, "
                    + "LOGIN VARCHAR(256)"
                    + ")";

    /**
     * Client creation query.
     */
    private static final String ADD_CLIENT =
            "INSERT INTO CLIENTS(ID, LOGIN) VALUES(?, ?)";

    /**
     * Order item creation query.
     */
    private static final String ADD_ITEM =
            "INSERT INTO ORDER_ITEMS "
            + "(CLIENT_ID, ORDER_ID, ITEM_ID)"
            + "VALUES(?, ?, ?)";

    /**
     * Do not construct me.
     */
    private DatabaseSetup() { }

    /**
     * Here we create tables and populate them with sample data.
     * @param db Database connection object.
     * @throws SQLException when something goes wrong.
     */
    public static void setUp(final Connection db) throws SQLException {
        try (Statement dataQuery = db.createStatement()) {
            dataQuery.execute(CREATE_ITEM);
            dataQuery.execute(CREATE_CLIENT);
        }
        try (PreparedStatement clientAddQuery
                     = db.prepareStatement(ADD_CLIENT)) {
            clientAddQuery.setInt(1, 1);
            clientAddQuery.setString(2, "test");
            clientAddQuery.addBatch();
            clientAddQuery.setInt(1, 2);
            clientAddQuery.setString(2, "login");
            clientAddQuery.addBatch();
            clientAddQuery.setInt(1, MAX_CLIENTS);
            clientAddQuery.setString(2, "example");
            clientAddQuery.addBatch();
            clientAddQuery.executeBatch();
        }

        try (PreparedStatement itemAddQuery = db.prepareStatement(ADD_ITEM)) {
            for (int c = 1; c <= MAX_CLIENTS; c++) {
                for (int i = 1; i <= MAX_ITEMS; i++) {
                    itemAddQuery.setInt(1, c);
                    itemAddQuery.setInt(2, c);
                    itemAddQuery.setInt(MAX_CLIENTS, i);
                    itemAddQuery.addBatch();
                }
            }
            itemAddQuery.executeBatch();
        }
    }

}

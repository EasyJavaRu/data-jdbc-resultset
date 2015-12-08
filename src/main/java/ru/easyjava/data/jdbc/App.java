package ru.easyjava.data.jdbc;

import com.sun.rowset.CachedRowSetImpl;
import com.sun.rowset.FilteredRowSetImpl;
import com.sun.rowset.JdbcRowSetImpl;
import com.sun.rowset.JoinRowSetImpl;
import com.sun.rowset.WebRowSetImpl;

import javax.sql.RowSet;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.FilteredRowSet;
import javax.sql.rowset.JdbcRowSet;
import javax.sql.rowset.JoinRowSet;
import javax.sql.rowset.Predicate;
import javax.sql.rowset.WebRowSet;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * JDBC statements example.
 */
public final class App {
    /**
     * ID of additional item.
     */
    private static final int ADDITIONAL_ITEM = 10;

    /**
     * We are going to modify that row.
     */
    private static final int FIFTH_ROW = 5;
    /**
     * Do not construct me.
     */
    private App() {
    }

    /**
     * Example of ResultSet usage.
     * @param db Database connection object.
     * @throws SQLException when soemthing goes wrong.
     */
    protected static void readResultSet(final Connection db)
            throws SQLException {
        System.out.println("Dumping ORDER_ITEMS table:");
        try (Statement results = db.createStatement()) {
            try (ResultSet rs =
                         results.executeQuery("SELECT * FROM ORDER_ITEMS")) {
                while (rs.next()) {
                    System.out.println(
                            String.format(
                                    "client=%d, order=%d, item=%d",
                                    rs.getInt("CLIENT_ID"),
                                    rs.getInt("ORDER_ID"),
                                    rs.getInt("ITEM_ID")));
                }
            }
        }
    }

    /**
     * Example of updatable ResultSet.
     * @param db Database connection object.
     * @throws SQLException when something goes wrong.
     */
    protected static void updateResultSet(final Connection db)
            throws SQLException {
        try (Statement updatableResult
                     = db.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_UPDATABLE)) {
            try (ResultSet rs =
                    updatableResult.executeQuery("SELECT * FROM ORDER_ITEMS")) {
                rs.absolute(FIFTH_ROW);
                rs.updateInt("CLIENT_ID", 2);
                rs.updateRow();

                rs.moveToInsertRow();
                rs.updateInt("CLIENT_ID", 1);
                rs.updateInt("ORDER_ID", 1);
                rs.updateInt("ITEM_ID", ADDITIONAL_ITEM);
                rs.insertRow();
            }
        }
    }

    /**
     * Example of JdbcRowSet usage.
     * @param db Database connection object.
     * @throws SQLException when something goes wrong.
     */
    protected static void jdbcRowSet(final Connection db)
            throws SQLException {
        JdbcRowSet rs = new JdbcRowSetImpl(db);
        rs.setCommand("SELECT * FROM ORDER_ITEMS");
        rs.execute();

        rs.moveToInsertRow();
        rs.updateInt("CLIENT_ID", 1);
        rs.updateInt("ORDER_ID", 1);
        rs.updateInt("ITEM_ID", ADDITIONAL_ITEM + 1);
        rs.insertRow();

        rs.execute();
        rs.beforeFirst();
        System.out.println("Dumping ORDER_ITEMS table using RowSet:");
        while (rs.next()) {
            System.out.println(
                    String.format(
                            "client=%d, order=%d, item=%d",
                            rs.getInt("CLIENT_ID"),
                            rs.getInt("ORDER_ID"),
                            rs.getInt("ITEM_ID")));
        }
    }

    /**
     * Exampl of CachedRowSet usage.
     * @param db Database connection object.
     * @return CachedRowSet object with data.
     * @throws SQLException when something goes wrong.
     */
    protected static CachedRowSet cachedRowSet(final Connection db)
            throws SQLException {
        try (Statement results = db.createStatement()) {
            ResultSet rs =
                    results.executeQuery("SELECT * FROM ORDER_ITEMS");
            CachedRowSet cs = new CachedRowSetImpl();
            cs.populate(rs);

            return cs;
        }
    }

    /**
     * Example of JoinRowSet usage.
     * @param db Database connection object.
     * @throws SQLException when something goes wrong.
     */
    protected static void joinRowSet(final Connection db)
            throws SQLException {
        CachedRowSet orders = new CachedRowSetImpl();
        CachedRowSet clients = new CachedRowSetImpl();
        try (Statement results = db.createStatement()) {
            try (ResultSet rs =
                    results.executeQuery("SELECT * FROM ORDER_ITEMS")) {
                orders.populate(rs);
            }
        }
        try (Statement results = db.createStatement()) {
            try (ResultSet rs =
                    results.executeQuery("SELECT * FROM CLIENTS")) {
                clients.populate(rs);
            }
        }
        JoinRowSet jrs = new JoinRowSetImpl();
        jrs.addRowSet(orders, "CLIENT_ID");
        jrs.addRowSet(clients, "ID");

        System.out.println("Dumping client logins and their items:");
        while (jrs.next()) {
            System.out.println(
                    String.format(
                            "client=%s, order=%d",
                            jrs.getString("LOGIN"),
                            jrs.getInt("ORDER_ID")));
        }
    }

    /**
     * Example of JDBC's Predicate interface implementation.
     */
    private static class ClientFilter implements Predicate {

        @Override
        public boolean evaluate(final RowSet rs) {
            try {
                return rs.getInt("CLIENT_ID") == DatabaseSetup.MAX_CLIENTS;
            } catch (SQLException e) {
                return false;
            }
        }

        @Override
        public boolean evaluate(final Object value, final int column)
                throws SQLException {
            return !(column == 1 && !"3".equals(value));

        }

        @Override
        public boolean evaluate(final Object value, final String columnName)
                throws SQLException {
            return !("CLIENT_ID".equals(columnName)
                    && !"3".equals((String) value));
        }
    }

    /**
     * Example of FilteredRowSet usage.
     * @param db Database connection object.
     * @throws SQLException when something goes wrong.
     */
    protected static void filteredRowSet(final Connection db)
            throws SQLException {
        try (Statement results = db.createStatement()) {
            try (ResultSet rs =
                    results.executeQuery("SELECT * FROM ORDER_ITEMS")) {
                FilteredRowSet fs = new FilteredRowSetImpl();
                fs.populate(rs);

                fs.setFilter(new ClientFilter());

                System.out.println("Dumping only 3rd client from ORDER_ITEMS:");
                while (fs.next()) {
                    System.out.println(
                            String.format(
                                    "client=%d, order=%d, item=%d",
                                    fs.getInt("CLIENT_ID"),
                                    fs.getInt("ORDER_ID"),
                                    fs.getInt("ITEM_ID")));
                }
            }
        }
    }

    /**
     * Example of webRowSet.
     * @param db Database connection object.
     * @throws SQLException when something goes wrong.
     * @throws IOException when not able to form XML.
     */
    protected static void webRowSet(final Connection db)
            throws SQLException, IOException {
        try (Statement results = db.createStatement()) {
            try (ResultSet rs =
                    results.executeQuery("SELECT * FROM ORDER_ITEMS")) {
                WebRowSet ws = new WebRowSetImpl();
                ws.populate(rs);

                ws.writeXml(System.out);
            }
        }

    }
    /**
     * Entry point.
     *
     * @param args Command line args. Not used.
     */
    public static void main(final String[] args) {
        CachedRowSet cs = null;
        try (Connection db = DriverManager.getConnection("jdbc:h2:mem:")) {
            DatabaseSetup.setUp(db);

            readResultSet(db);
            updateResultSet(db);
            readResultSet(db);
            jdbcRowSet(db);
            joinRowSet(db);
            filteredRowSet(db);
            webRowSet(db);

            cs = cachedRowSet(db);
        } catch (SQLException ex) {
            System.out.println("Database connection failure: "
                    + ex.getMessage());
        } catch (IOException ex) {
            System.out.println("I/O error: "
                    + ex.getMessage());
        }
        try {
            System.out.println("Dumping ORDER_ITEMS without db connection:");
            assert cs != null;
            while (cs.next()) {
                System.out.println(
                        String.format(
                                "client=%d, order=%d, item=%d",
                                cs.getInt("CLIENT_ID"),
                                cs.getInt("ORDER_ID"),
                                cs.getInt("ITEM_ID")));
            }
        } catch (SQLException ex) {
            System.out.println("Database connection failure: "
                    + ex.getMessage());
        }
    }
}

package com.company.generators;

import com.company.models.*;
import com.company.parsers.XmlParser;
import com.mysql.cj.jdbc.MysqlDataSource;
import org.xml.sax.SAXException;

import javax.management.modelmbean.XMLParseException;
import javax.xml.crypto.dsig.XMLSignatureException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.function.ToDoubleBiFunction;

public class DataGenerator {
    private static final Random RANDOM = new Random(System.currentTimeMillis() % 1000);

    private static final int MAX_STRING_LENGTH = 20;

    private static final byte ASCII_MIN_BOUND = 32;

    private static final byte ASCII_CHARACTERS_COUNT = 90;

    private static Hashtable<String, Supplier<Object>> NON_NUMERIC_TYPES = new Hashtable<String, Supplier<Object>>() {
        {
            put("string", () -> {
                byte[] str = new byte[MAX_STRING_LENGTH];
                RANDOM.nextBytes(str);
                for (int i = 0; i < str.length; ++i) {
                    str[i] = (byte)(str[i] % ASCII_MIN_BOUND + ASCII_CHARACTERS_COUNT);
                }
                return "\"" + new String(str) + "\"";
            });
            put("boolean", () -> RANDOM.nextBoolean());
        }
    };

    private static double getDistributedValue(double mean, double dispersion)
    {
        return RANDOM.nextGaussian() * mean * dispersion / 100.0 + mean;
    }

    private static Hashtable<String, ToDoubleBiFunction<Double, Double>> NUMERIC_TYPES = new Hashtable<String, ToDoubleBiFunction<Double, Double>>() {
        {
            put("integer", (mean, dispersion) -> (int)(getDistributedValue(mean, dispersion)));
            put("double", (mean, dispersion) -> getDistributedValue(mean, dispersion));
        }
    };

    private static final Hashtable<String, String> ABSTRACT_TYPES_TO_DATABASE_TYPES = new Hashtable<String, String>() {
        {
            put("string", "VARCHAR(100) NOT NULL");
            put("integer", "INTEGER NOT NULL");
            put("double", "DOUBLE NOT NULL");
            put("boolean", "BOOLEAN NOT NULL");
        }
    };

    private static final IntFunction<Integer> GET_FOREIGN_KEY = (maxValue) -> (Math.abs(RANDOM.nextInt()) % maxValue) + 1;

    private static boolean fillTable(Connection connection, SqlTable table)
    {
        SqlColumn[] columns = table.getTableColumns();
        StringBuilder columnNames = new StringBuilder(columns[0].getColumnName());
        StringBuilder values = new StringBuilder("?");
        for (int i = 1; i < columns.length; ++i) {
            columnNames.append(",").append(columns[i].getColumnName());
            values.append(",?");
        }
        int mean = table.getMean();
        double dispersion = table.getDispersionPercentage();
        int recordsCount = mean + RANDOM.nextInt() % (int)(mean * dispersion / 100.0);
        String insertQuery = "INSERT INTO " + table.getTableName() + " (" + columnNames.toString() + ") VALUES ("
                + values.toString() + ")";
        try {
            PreparedStatement s = connection.prepareStatement(insertQuery);
            boolean result = true;
            for (int i = 0; i < recordsCount && result; ++i) {
                int nonRefColumnsCount = columns.length;
                for (int j = 0; j < nonRefColumnsCount; ++j) {
                    if (columns[j] instanceof SqlNumericColumn) {
                        SqlNumericColumn c = (SqlNumericColumn)(columns[j]);
                        s.setObject(j + 1, NUMERIC_TYPES.get(c.getColumnType()).applyAsDouble(c.getMean(), c.getDispersionPercentage()));
                    } else {
                        s.setObject(j + 1, NON_NUMERIC_TYPES.get(columns[j].getColumnType()).get());
                    }
                }
                if (s.executeUpdate() == 0) {
                    throw new SQLException("Cannot insert another row");
                }
            }
        } catch (SQLException e) {
            return false;
        }
        return true;
    }

    private static boolean databaseExists(Connection connection, String databaseName) throws
            SQLException
    {
        Statement s = connection.createStatement();
        return s.executeQuery("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '" + databaseName + "'").next();
    }

    private static void createTableScheme(Connection connection, SqlTable table) throws
            SQLException
    {
        StringBuilder createTableQuery = new StringBuilder("CREATE TABLE ");
        String tableName = table.getTableName();
        createTableQuery.append(tableName).append("(id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT");
        for (SqlColumn column : table.getTableColumns()) {
            createTableQuery
                    .append(',')
                    .append(column.getColumnName())
                    .append(' ')
                    .append(ABSTRACT_TYPES_TO_DATABASE_TYPES.get(column.getColumnType()));
        }
        for (Reference r : table.getForeignKeys()) {
            createTableQuery.append(',').append(r.getColumnName()).append(' ').append("INTEGER");
        }
        createTableQuery.append(')');
        Statement s = connection.createStatement();
        s.executeUpdate(createTableQuery.toString());
        s.close();
    }

    private static boolean fillForeignKeys(Connection connection, SqlTable table) throws SQLException {
        Reference[] refs = table.getForeignKeys();
        int refCount = refs.length;
        int[] maxId = new int[refCount];
            Statement temp = connection.createStatement();
            for (int i = 0; i < refCount; ++i) {
                ResultSet rs = temp.executeQuery("SELECT MAX(id) FROM " + refs[i].getTableName());
                rs.next();
                maxId[i] = rs.getInt(1);
            }
            String tableName = table.getTableName();
            StringBuilder updateQuery = new StringBuilder("UPDATE " + tableName + " SET ");
            for (Reference ref : refs) {
                updateQuery.append(ref.getColumnName()).append("=?,");
            }
            updateQuery.deleteCharAt(updateQuery.length() - 1);
            updateQuery.append(" WHERE id = ?");
            PreparedStatement ps = connection.prepareStatement(updateQuery.toString());
            ResultSet rs = temp.executeQuery("SELECT COUNT(*) FROM " + tableName);
            rs.next();
            int rowCount = rs.getInt(1);
            temp.close();
            for (int i = 0; i < rowCount; ++i) {
                for (int j = 0; j < refCount; ++j) {
                    ps.setInt(j + 1, GET_FOREIGN_KEY.apply(maxId[j]));
                }
                ps.setInt(refCount + 1, i + 1);
                ps.executeUpdate();
            }
        return true;
    }

    public static void generateDatabase(String connectionPropertiesFilePath, String tableDeclarationFilePath) throws
            DatabaseGenerationException,
            IOException,
            SAXException,
            SQLException,
            XMLParseException,
            XMLSignatureException
    {
        SqlDatabase database = XmlParser.fromFile(tableDeclarationFilePath);
        Properties props = new Properties();
        props.load(new FileInputStream(new File(connectionPropertiesFilePath)));
        MysqlDataSource ds = new MysqlDataSource();
        ds.setServerName(props.getProperty("server"));
        ds.setUser(props.getProperty("username"));
        ds.setPassword(props.getProperty("password"));
        Connection conn = ds.getConnection();
        String databaseName = database.getDatabaseName();
        if (databaseExists(conn, databaseName)) {
            throw new DatabaseGenerationException("Database \"" + databaseName + "\" already exists");
        }
        Statement s = conn.createStatement();
        s.executeUpdate("CREATE DATABASE " + databaseName);
        s.executeUpdate("USE " + databaseName);
        SqlTable[] tables = database.getDatabaseTables();
        conn.setAutoCommit(false);
        for (SqlTable t : tables) {
            createTableScheme(conn, t);
            if (!fillTable(conn, t)) {
                conn.rollback();
                conn.close();
                return;
            }
        }
        for (SqlTable t : tables) {
            if (!fillForeignKeys(conn, t)) {
                conn.rollback();
                conn.close();
                return;
            }
        }
        for (SqlTable t : tables) {
            String tableName = t.getTableName();
            for (Reference r : t.getForeignKeys()) {
                s.executeUpdate("ALTER TABLE " + tableName + " ADD CONSTRAINT FOREIGN KEY (" + r.getColumnName()
                        + ") REFERENCES " + r.getTableName() + "(id)");
            }
        }
        conn.commit();
        conn.close();
    }
}

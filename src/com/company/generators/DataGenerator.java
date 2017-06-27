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

    private static final String FOREIGN_KEY_COLUMN_TYPE = "INTEGER NOT NULL";

    private static final Hashtable<String, String> ABSTRACT_TYPES_TO_DATABASE_TYPES = new Hashtable<String, String>() {
        {
            put("string", "VARCHAR(100) NOT NULL");
            put("integer", "INTEGER NOT NULL");
            put("double", "DOUBLE NOT NULL");
            put("boolean", "BOOLEAN NOT NULL");
        }
    };

    private static final IntFunction<Integer> GET_FOREIGN_KEY = (maxValue) -> (Math.abs(RANDOM.nextInt()) % maxValue) + 1;

    private static byte[][] makeMatrixClone(byte[][] matrix)
    {
        int length = matrix.length;
        byte[][] result = new byte[length][];
        for (int i = 0; i < length; ++i) {
            result[i] = matrix[i].clone();
        }
        return result;
    }

    static private boolean isCycledGraph(byte[][] adjacencyMatrix)
    {
        byte[][] multipliedMatrix = makeMatrixClone(adjacencyMatrix);
        byte[][] multiplier = makeMatrixClone(adjacencyMatrix);
        int length = adjacencyMatrix.length;
        for (int pathLength = 1; pathLength <= length; ++pathLength) {
            byte[][] temp = makeMatrixClone(multipliedMatrix);
            for (int i = 0; i < length; ++i) {
                for (int j = 0; j < length; ++j) {
                    temp[i][j] = 0;
                    for (int k = 0; k < length; ++k) {
                        temp[i][j] += multipliedMatrix[i][k] * multiplier[k][j];
                    }
                }
            }
            int sumDiag = 0;
            for (int i = 0; i < length; ++i) {
                sumDiag += temp[i][i];
            }
            if (sumDiag / pathLength != 0) {
                return true;
            }
            multipliedMatrix = temp;
        }
        return false;
    }

    private static boolean isCycledTableDeclaration(SqlTable[] tables)
    {
        int tablesCount = tables.length;
        byte[][] adjacencyMatrix = new byte[tablesCount][tablesCount];
        for (int i = 0; i < tablesCount; ++i) {
            Reference[] refs = tables[i].getForeignKeys();
            Set<String> refNames = new HashSet<String>() {
                {
                    for (Reference ref : refs) {
                        add(ref.getTableName());
                    }
                }
            };
            for (int j = 0; j < tablesCount; ++j) {
                adjacencyMatrix[i][j] = (byte)(refNames.contains(tables[j].getTableName()) ? 1 : 0);
            }
        }
        return isCycledGraph(adjacencyMatrix);
    }

    private static SqlTable[] getTablesInOrderOfCreation(SqlTable[] tables)
    {
        LinkedList<SqlTable> temp = new LinkedList<>(Arrays.asList(tables));
        int currentItemIndex = 0;
        int itemsCount = temp.size();
        int lastItemIndex = itemsCount - 1;
        HashSet<SqlTable> tablesSet = new HashSet<>();
        while (currentItemIndex < lastItemIndex) {
            SqlTable t = temp.get(currentItemIndex);
            if (tablesSet.contains(t)) {
                ++currentItemIndex;
                continue;
            } else {
                tablesSet.add(t);
            }
            int i = lastItemIndex;
            final int index = currentItemIndex;
            Set<String> refNames = new HashSet<String>() {
                {
                    for (Reference ref : temp.get(index).getForeignKeys()) {
                        add(ref.getTableName());
                    }
                }
            };
            while (i > currentItemIndex && !refNames.contains(temp.get(i).getTableName())) {
                --i;
            }
            if (i == currentItemIndex) {
                ++currentItemIndex;
            } else {
                temp.add(i + 1, temp.get(currentItemIndex));
                temp.remove(currentItemIndex);
            }
        }
        SqlTable[] result = new SqlTable[itemsCount];
        temp.toArray(result);
        return result;
    }

    private static boolean fillTable(Connection connection, SqlTable table) throws
            SQLException
    {
        SqlColumn[] columns = table.getTableColumns();
        StringBuilder columnNames = new StringBuilder(columns[0].getColumnName());
        StringBuilder values = new StringBuilder("?");
        for (int i = 1; i < columns.length; ++i) {
            columnNames.append(",").append(columns[i].getColumnName());
            values.append(",?");
        }
        Reference[] refs = table.getForeignKeys();
        int refsCount = refs.length;
        int[] maxIdValues = new int[refs.length];
        for (int i = 0; i < refsCount; ++i) {
            columnNames.append(",").append(refs[i].getColumnName());
            values.append(",?");
            PreparedStatement s = connection.prepareStatement("SELECT MAX(id) FROM " + refs[i].getTableName());
            ResultSet rs = s.executeQuery();
            rs.next();
            maxIdValues[i] = rs.getInt(1);
        }
        int mean = table.getMean();
        double dispersion = table.getDispersionPercentage();
        int recordsCount = mean + RANDOM.nextInt() % (int)(mean * dispersion / 100.0);
        String insertQuery = "INSERT INTO " + table.getTableName() + " (" + columnNames.toString() + ") VALUES ("
                + values.toString() + ")";
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
            for (int j = 0; j < refsCount; ++j) {
                s.setObject(nonRefColumnsCount + j + 1, GET_FOREIGN_KEY.apply(maxIdValues[j]));
            }
            if (s.executeUpdate() == 0) {
                result = false;
            }
        }
        return result;
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
        Reference[] refs = table.getForeignKeys();
        int refsCount = refs.length;
        String[] fkQueries = new String[refsCount];
        for (int i = 0; i < refsCount; ++i) {
            String refColumnName = refs[i].getColumnName();
            createTableQuery.append(',').append(refColumnName).append(' ').append(FOREIGN_KEY_COLUMN_TYPE);
            fkQueries[i] = "ALTER TABLE " + tableName + " ADD CONSTRAINT FOREIGN KEY (" + refColumnName +
                    ") REFERENCES " + refs[i].getTableName() + "(id)";
        }
        createTableQuery.append(')');
        Statement s = connection.createStatement();
        s.executeUpdate(createTableQuery.toString());
        for (String query : fkQueries) {
            s.executeUpdate(query);
        }
        s.close();
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
        SqlTable[] tables = database.getDatabaseTables();
        if (isCycledTableDeclaration(tables)) {
            throw new DatabaseGenerationException("Tables are declared cyclically");
        }
        SqlTable[] orderedTables = getTablesInOrderOfCreation(tables);
        Properties props = new Properties();
        props.load(new FileInputStream(new File(connectionPropertiesFilePath)));
        MysqlDataSource ds = new MysqlDataSource();
        ds.setServerName(props.getProperty("server"));
        ds.setUser(props.getProperty("username"));
        ds.setPassword(props.getProperty("password"));
        Connection conn = ds.getConnection();
        String databaseName = database.getDatabaseName();
        conn.setAutoCommit(false);
        if (databaseExists(conn, databaseName)) {
            throw new DatabaseGenerationException("Database \"" + databaseName + "\" already exists");
        }
        Statement s = conn.createStatement();
        s.executeUpdate("CREATE DATABASE " + databaseName);
        s.executeUpdate("USE " + databaseName);
        s.close();
        boolean result = true;
        for (SqlTable t : orderedTables) {
            createTableScheme(conn, t);
            result = fillTable(conn, t);
            if (!result) {
                break;
            }
        }
        if (result) {
            conn.commit();
        } else {
            conn.rollback();
        }
        conn.close();
    }
}

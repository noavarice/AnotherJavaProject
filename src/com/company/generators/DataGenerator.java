package com.company.generators;

import com.company.models.SqlColumn;
import com.company.models.SqlTable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.Random;
import java.util.function.Supplier;

public class DataGenerator {
    private static final Random RANDOM = new Random(System.currentTimeMillis() % 1000);

    private static final int MAX_STRING_LENGTH = 100;

    private static Hashtable<String, Supplier<Object>> TYPE_TO_VALUE = new Hashtable<String, Supplier<Object>>() {
        {
            put("string", () -> {
                byte[] str = new byte[MAX_STRING_LENGTH];
                RANDOM.nextBytes(str);
                return "\"" + new String(str) + "\"";
            });
            put("int", () -> RANDOM.nextInt());
            put("double", () -> RANDOM.nextDouble());
            put("boolean", () -> RANDOM.nextBoolean());
        }
    };

    public static boolean fillTable(Connection connection, SqlTable table) throws
            SQLException
    {
        SqlColumn[] columns = table.getTableColumns();
        StringBuilder columnNames = new StringBuilder(columns[0].getColumnName());
        StringBuilder values = new StringBuilder("?");
        for (int i = 1; i < columns.length; ++i) {
            columnNames.append(",").append(columns[i].getColumnName());
            values.append(",?");
        }
        String query = "INSERT INTO " + table.getTableName() + " (" + columnNames.toString() + ") VALUES ("
                + values.toString() + ")";
        PreparedStatement s = connection.prepareStatement(query);
        int mean = table.getMean();
        double dispersion = table.getDispersionPercentage();
        int recordsCount = mean + RANDOM.nextInt() % (int)(mean * dispersion);
        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        boolean result = true;
        for (int i = 0; i < recordsCount && result; ++i) {
            for (int j = 1; j <= columns.length; ++j) {
                s.setObject(j, TYPE_TO_VALUE.get(columns[j].getColumnType()).get());
            }
            if (s.executeUpdate() == 0) {
                result = false;
            }
        }
        if (result) {
            connection.commit();
        } else {
            connection.rollback();
        }
        connection.setAutoCommit(autoCommit);
        return result;
    }
}

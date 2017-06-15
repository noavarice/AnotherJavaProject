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

    private static final int MAX_STRING_LENGTH = 20;

    private static final byte ASCII_MIN_BOUND = 32;

    private static final byte ASCII_CHARACTERS_COUNT = 90;

    private static Hashtable<String, Supplier<Object>> TYPE_TO_VALUE = new Hashtable<String, Supplier<Object>>() {
        {
            put("string", () -> {
                byte[] str = new byte[MAX_STRING_LENGTH];
                RANDOM.nextBytes(str);
                for (int i = 0; i < str.length; ++i) {
                    str[i] = (byte)(str[i] % ASCII_MIN_BOUND + ASCII_CHARACTERS_COUNT);
                }
                return "\"" + new String(str) + "\"";
            });
            put("integer", () -> RANDOM.nextInt());
            put("double", () -> RANDOM.nextGaussian() * 1000000);
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
        int recordsCount = mean + RANDOM.nextInt() % (int)(mean * dispersion / 100.0);
        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        boolean result = true;
        for (int i = 0; i < recordsCount && result; ++i) {
            for (int j = 0; j < columns.length; ++j) {
                s.setObject(j + 1, TYPE_TO_VALUE.get(columns[j].getColumnType()).get());
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

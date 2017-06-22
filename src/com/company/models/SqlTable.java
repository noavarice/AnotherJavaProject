package com.company.models;

import java.util.Collection;
import java.util.Set;

public class SqlTable {
    private String name;

    private final SqlColumn[] columns;

    private final Set<String> refs;

    private int mean;

    private double dispersion;

    public SqlTable(String tableName, SqlColumn[] tableColumns, Set<String> foreignKeys, int mean, double dispersionPercentage)
    {
        name = tableName;
        columns = tableColumns;
        this.mean = mean;
        dispersion = dispersionPercentage;
        refs = foreignKeys;
    }

    public String getTableName()
    {
        return name;
    }

    public SqlColumn[] getTableColumns()
    {
        return columns;
    }

    public int getMean()
    {
        return mean;
    }

    public double getDispersionPercentage()
    {
        return dispersion;
    }

    public Set<String> getForeignKeys()
    {
        return refs;
    }
}

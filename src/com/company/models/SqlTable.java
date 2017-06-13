package com.company.models;

import java.util.Collection;

public class SqlTable {
    private String name;

    private SqlColumn columns[] = null;

    private int mean;

    private double dispersion;

    public SqlTable(String tableName, Collection<SqlColumn> tableColumns, int mean, double dispersionPercentage)
    {
        name = tableName;
        columns = (SqlColumn[])(tableColumns.toArray());
        this.mean = mean;
        dispersion = dispersionPercentage;
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
}

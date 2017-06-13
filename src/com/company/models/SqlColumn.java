package com.company.models;

public class SqlColumn {
    private String name;

    private String type;

    private boolean isPk;

    public SqlColumn(String columnName, String columnType, boolean isPrimaryKey)
    {
        name = columnName;
        type = columnType;
        isPk = isPrimaryKey;
    }

    public String getColumnName()
    {
        return name;
    }

    public String getColumnType()
    {
        return type;
    }

    public boolean isPrimaryKey()
    {
        return isPk;
    }
}

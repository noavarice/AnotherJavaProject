package com.company.models;

public class SqlColumn {
    private String name;

    private String type;

    public SqlColumn(String columnName, String columnType)
    {
        name = columnName;
        type = columnType;
    }

    public String getColumnName()
    {
        return name;
    }

    public String getColumnType()
    {
        return type;
    }
}

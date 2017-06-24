package com.company.models;

public class Reference {

    private String tableName;

    private String columnName;

    public Reference(String tableName, String columnName)
    {
        this.tableName = tableName;
        this.columnName = columnName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public String getColumnName()
    {
        return columnName;
    }
}

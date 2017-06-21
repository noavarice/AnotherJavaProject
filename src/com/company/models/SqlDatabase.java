package com.company.models;

public class SqlDatabase {
    private String name;

    private final SqlTable[] tables;

    public SqlDatabase(String databaseName, SqlTable[] databaseTables)
    {
        name = databaseName;
        tables = databaseTables;
    }

    public String getDatabaseName()
    {
        return name;
    }

    public SqlTable[] getDatabaseTables()
    {
        return tables;
    }
}

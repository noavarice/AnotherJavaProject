package com.company.models;

import java.util.Collection;

public class SqlTable {
    private String name;

    private SqlColumn columns[] = null;

    public SqlTable(String tableName, Collection<SqlColumn> tableColumns)
    {
        name = tableName;
        columns = (SqlColumn[])(tableColumns.toArray());
    }

    public String getTableName()
    {
        return name;
    }

    public SqlColumn[] getTableColumns()
    {
        return columns;
    }
}

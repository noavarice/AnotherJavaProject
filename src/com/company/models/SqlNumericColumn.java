package com.company.models;

public class SqlNumericColumn extends SqlColumn {

    private double mean;

    private double dispersion;

    public SqlNumericColumn(String columnName, String columnType, double meanValue, double dispersionPercentage)
    {
        super(columnName, columnType);
        mean = meanValue;
        dispersion = dispersionPercentage;
    }

    public double getMean()
    {
        return mean;
    }

    public double getDispersionPercentage()
    {
        return dispersion;
    }
}

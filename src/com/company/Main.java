package com.company;

import com.company.models.SqlColumn;
import com.company.models.SqlTable;
import com.company.parsers.XmlParser;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.Collection;

public class Main {

    public static void main(String[] args) throws IOException, SAXException {
        Collection<SqlTable> tables = XmlParser.fromFile("/home/alexrazinkov/Projects/Java/xml_test");
        if (tables == null) {
            System.out.println("No tables");
            return;
        }
        for (SqlTable t : tables) {
            SqlColumn[] columns = t.getTableColumns();
            System.out.println(t.getTableName());
            for (SqlColumn c : columns) {
                System.out.println(
                        "    {" + c.getColumnName() + ", "
                                + c.getColumnType() + ", "
                                + (c.isPrimaryKey() ? "true}" : "false}"));
            }
        }
    }
}

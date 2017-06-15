package com.company;

import com.company.generators.DataGenerator;
import com.company.models.SqlTable;
import com.company.parsers.XmlParser;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Properties;

public class Main {

    public static void main(String[] args) throws IOException, SAXException, SQLException {
        Collection<SqlTable> tables = XmlParser.fromFile("/home/alexrazinkov/Projects/Java/xml_test");
        if (tables == null) {
            System.out.println("No tables");
            return;
        }
        Properties props = new Properties();
        props.load(new FileInputStream(new File("/home/alexrazinkov/Projects/Java/conn")));
        MysqlDataSource ds = new MysqlDataSource();
        ds.setServerName(props.getProperty("server"));
        ds.setDatabaseName(props.getProperty("database"));
        ds.setUser(props.getProperty("username"));
        ds.setPassword(props.getProperty("password"));
        Connection conn = ds.getConnection();
        boolean result;
        for (SqlTable t : tables) {
            result = DataGenerator.fillTable(conn, t);
            if (!result) {
                System.out.println("Failed");
                return;
            }
        }
        System.out.println("Succeded");
    }
}

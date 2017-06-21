package com.company;

import com.company.generators.DataGenerator;
import com.company.generators.DatabaseGenerationException;
import org.xml.sax.SAXException;

import javax.management.modelmbean.XMLParseException;
import javax.xml.crypto.dsig.XMLSignatureException;
import java.io.IOException;
import java.sql.SQLException;

public class Main {

    public static void main(String[] args) throws
            IOException,
            SAXException,
            SQLException,
            XMLSignatureException,
            XMLParseException,
            DatabaseGenerationException
    {
        String connectionPropertiesFilePath = "/home/alexrazinkov/Projects/Java/conn";
        String tableDeclarationsFilePath = "/home/alexrazinkov/Projects/Java/xml_test";
        try {
            DataGenerator.generateDatabase(connectionPropertiesFilePath, tableDeclarationsFilePath);
        } catch (XMLSignatureException e) {
            System.out.println(e.getMessage());
        } catch (XMLParseException e) {
            System.out.println(e.getMessage());
        }
    }
}

package com.company.parsers;

import com.company.models.SqlColumn;
import com.company.models.SqlTable;
import com.sun.org.apache.xerces.internal.parsers.DOMParser;
import org.w3c.dom.*;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;

public class XmlParser {

    public static Collection<SqlTable> fromFile(String filePath) throws
            IOException,
            SAXException
    {
        DOMParser parser = new DOMParser();
        parser.parse(filePath);
        Document doc = parser.getDocument();
        Element root = doc.getDocumentElement();
        NodeList childNodes = root.getElementsByTagName("table");
        LinkedList<SqlTable> result = new LinkedList<>();
        for (int i = 0; i < childNodes.getLength(); ++i) {
            Element item = (Element)(childNodes.item(i));
            NodeList columnNodes = item.getElementsByTagName("column");
            int columnCount = columnNodes.getLength();
            SqlColumn[] columns = new SqlColumn[columnCount];
            for (int j = 0; j < columnCount; ++j) {
                if (columnNodes.item(j).getNodeType() != Node.ELEMENT_NODE) {
                    continue;
                }
                NamedNodeMap attrs = columnNodes.item(j).getAttributes();
                String name = attrs.getNamedItem("name").getNodeValue();
                String type = attrs.getNamedItem("type").getNodeValue();
                String isPk = attrs.getNamedItem("isPrimaryKey").getNodeValue();
                columns[j] = new SqlColumn(name, type, (isPk == null || isPk == "false") ? false : true);
            }
            NamedNodeMap attrs = item.getAttributes();
            result.addLast(new SqlTable(attrs.getNamedItem("name").getNodeValue(), Arrays.asList(columns)));
        }
        return result;
    }
}

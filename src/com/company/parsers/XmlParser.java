package com.company.parsers;

import com.company.models.SqlColumn;
import com.company.models.SqlTable;
import com.sun.org.apache.xerces.internal.parsers.DOMParser;
import org.w3c.dom.*;
import org.xml.sax.SAXException;

import javax.management.modelmbean.XMLParseException;
import javax.xml.crypto.dsig.XMLSignatureException;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

public class XmlParser {
    private static final int DEFAULT_MEAN = 1000;

    private static final double DEFAULT_DISPERSION_PERCENTAGE = 10.0;

    private static final Pattern CHECK_NAME = Pattern.compile("\\w+");

    private static final Set<String> ALLOWED_TYPES = new HashSet<String>() {
        {
            add("string");
            add("integer");
            add("double");
            add("boolean");
        }
    };

    private static final String RESERVED_ID_NAME = "id";

    private static SqlColumn[] getColumns(Element rootItem)
    {
        NodeList columnNodes = rootItem.getElementsByTagName("column");
        int columnCount = columnNodes.getLength();
        if (columnCount == 0) {
            return null;
        }
        SqlColumn[] result = new SqlColumn[columnCount];
        TreeSet<String> uniqueColumnNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (int j = 0; j < columnCount; ++j) {
            if (columnNodes.item(j).getNodeType() != Node.ELEMENT_NODE) {
                continue;
            }
            NamedNodeMap attrs = columnNodes.item(j).getAttributes();
            String name, type;
            try {
                name = attrs.getNamedItem("name").getNodeValue().trim();
                if (!CHECK_NAME.matcher(name).matches()) {
                    return null;
                }
                if (name.equalsIgnoreCase(RESERVED_ID_NAME)) {
                    return null;
                }
                if (uniqueColumnNames.contains(name)) {
                    return null;
                } else {
                    uniqueColumnNames.add(name);
                }
                type = attrs.getNamedItem("type").getNodeValue().trim().toLowerCase();
                if (!ALLOWED_TYPES.contains(type)) {
                    return null;
                }
            } catch (NullPointerException e) {
                return null;
            }
            result[j] = new SqlColumn(name, type);
        }
        return result;
    }

    private static Set<String> getTableReferences(Element rootItem)
    {
        NodeList l = rootItem.getElementsByTagName("reference");
        if (l == null) {
            return new HashSet<>();
        }
        int length = l.getLength();
        Set<String> result = new HashSet<>();
        for (int i = 0; i < length; i++) {
            Node n = l.item(i);
            if (n.getNodeType() != Node.ELEMENT_NODE) {
                continue;
            }
            result.add(n.getAttributes().getNamedItem("table").getNodeValue());
        }
        return result;
    }

    public static SqlTable[] fromFile(String filePath) throws
            IOException,
            SAXException,
            XMLSignatureException,
            XMLParseException
    {
        DOMParser parser = new DOMParser();
        parser.parse(filePath);
        Document doc = parser.getDocument();
        Element root = doc.getDocumentElement();
        NodeList childNodes = root.getElementsByTagName("table");
        TreeSet<String> tableNames = new TreeSet<>();
        int tablesCount = childNodes.getLength();
        SqlTable[] result = new SqlTable[tablesCount];
        for (int i = 0; i < tablesCount; ++i) {
            Element item = (Element)(childNodes.item(i));
            NamedNodeMap attrs = item.getAttributes();
            Node nameAttr = attrs.getNamedItem("name");
            if (nameAttr == null) {
                throw new XMLParseException("Table must have a name");
            }
            String name = nameAttr.getNodeValue();
            if (!CHECK_NAME.matcher(name).matches()) {
                throw new XMLParseException(name + ": invalid table name");
            }
            if (tableNames.contains(name)) {
                throw new XMLParseException("Tables must have unique names");
            } else {
                tableNames.add(name);
            }
            Node meanAttr = attrs.getNamedItem("mean");
            int mean = meanAttr == null ? DEFAULT_MEAN : Integer.valueOf(meanAttr.getNodeValue());
            if (mean < 1) {
                throw new XMLParseException(name + ": mean cannot be lesser then 1");
            }
            Node dispAttr = attrs.getNamedItem("dispersion");
            double dispersion = dispAttr == null ? DEFAULT_DISPERSION_PERCENTAGE : Double.valueOf(dispAttr.getNodeValue());
            if (dispersion < 0 || dispersion > 100.0) {
                throw new XMLParseException(name + ": dispersion value must belong to [0, 100] interval");
            }
            SqlColumn[] columns = getColumns(item);
            if (columns == null) {
                throw new XMLSignatureException(new Throwable(name + ": cannot create table without user-defined columns"));
            }
            result[i] = new SqlTable(name, Arrays.asList(columns), getTableReferences(item), mean, dispersion);
        }
        for (SqlTable t : result) {
            for (String ref : t.getForeignKeys()) {
                if (!tableNames.contains(ref)) {
                    throw new XMLParseException("Table \"" + t.getTableName() + "\" references to non-existing table \"" + ref + "\"");
                }
            }
        }
        return result;
    }
}

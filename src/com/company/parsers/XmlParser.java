package com.company.parsers;

import com.company.models.SqlColumn;
import com.company.models.SqlDatabase;
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

    private static final String FOREIGN_KEY_COLUMN_SUFFIX = "ID";

    private static SqlColumn[] getColumns(Element rootItem, TreeSet<String> reservedNames) throws
            XMLParseException
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
            String name = attrs.getNamedItem("name").getNodeValue().trim();
            if (name == null) {
                throw new XMLParseException("Column must have a name");
            }
            if (!CHECK_NAME.matcher(name).matches()) {
                throw new XMLParseException("Column name \"" + name + "\" is invalid");
            }
            if (reservedNames.contains(name)) {
                throw new XMLParseException("Column name \"" + name + "\" is reserved for PK or column referenced in FK");
            }
            if (uniqueColumnNames.contains(name)) {
                throw new XMLParseException("Column with name \"" + name + "\" has been defined multiple times");
            } else {
                uniqueColumnNames.add(name);
            }
            String type = attrs.getNamedItem("type").getNodeValue().trim().toLowerCase();
            if (type == null) {
                throw new XMLParseException("Column \"" + name + "\" must have a type");
            }
            if (!ALLOWED_TYPES.contains(type)) {
                throw new XMLParseException("Type of column \"" + name + "\" is not recognized");
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

    public static SqlDatabase fromFile(String filePath) throws
            IOException,
            SAXException,
            XMLSignatureException,
            XMLParseException
    {
        DOMParser parser = new DOMParser();
        parser.parse(filePath);
        Document doc = parser.getDocument();
        Element root = doc.getDocumentElement();
        NamedNodeMap databaseAttrs = root.getAttributes();
        Node databaseName = databaseAttrs.getNamedItem("name");
        if (databaseAttrs == null) {
            throw new XMLParseException("Database must have name");
        }
        NodeList childNodes = root.getElementsByTagName("table");
        TreeSet<String> tableNames = new TreeSet<>();
        int tablesCount = childNodes.getLength();
        SqlTable[] tables = new SqlTable[tablesCount];
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
            Set<String> refs = getTableReferences(item);
            TreeSet<String> reservedColumnNames = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER) {
                {
                    add(RESERVED_ID_NAME);
                    for (String s : refs) {
                        add(s + FOREIGN_KEY_COLUMN_SUFFIX);
                    }
                }
            };
            SqlColumn[] columns = getColumns(item, reservedColumnNames);
            if (columns == null && refs.isEmpty()) {
                throw new XMLParseException("Table \"" + name + "\" has no columns nor references to another tables");
            }
            tables[i] = new SqlTable(name, columns, refs, mean, dispersion);
        }
        for (SqlTable t : tables) {
            for (String ref : t.getForeignKeys()) {
                if (!tableNames.contains(ref)) {
                    throw new XMLParseException("Table \"" + t.getTableName() + "\" references to non-existing table \"" + ref + "\"");
                }
            }
        }
        return new SqlDatabase(databaseName.getNodeValue(), tables);
    }
}

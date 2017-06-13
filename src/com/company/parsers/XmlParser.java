package com.company.parsers;

import com.company.models.SqlColumn;
import com.company.models.SqlTable;
import com.sun.org.apache.xerces.internal.parsers.DOMParser;
import org.w3c.dom.*;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

public class XmlParser {
    private static final Pattern CHECK_NAME = Pattern.compile("\\w+");

    private static final Set<String> ALLOWED_TYPES = new HashSet<String>() {
        {
            add("string");
            add("integer");
            add("float");
            add("date");
        }
    };

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
            if (columnCount == 0) {
                return null;
            }
            SqlColumn[] columns = new SqlColumn[columnCount];
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
                Node isPkNode = attrs.getNamedItem("isPrimaryKey");
                String isPk = isPkNode == null ? "false" : isPkNode.getNodeValue().toLowerCase();
                if (!isPk.equals("false") && !isPk.equals("true")) {
                    return null;
                }
                columns[j] = new SqlColumn(name, type, isPk.equals("true"));
            }
            NamedNodeMap attrs = item.getAttributes();
            result.addLast(new SqlTable(attrs.getNamedItem("name").getNodeValue(), Arrays.asList(columns)));
        }
        return result;
    }
}

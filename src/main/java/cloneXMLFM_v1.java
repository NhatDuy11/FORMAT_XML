import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.yaml.snakeyaml.Yaml;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.util.*;
@UdfDescription(name = "ATM_CORE_V3", description = "Transform XML source_ATM_CORE")

public class cloneXMLFM_v1 {
    public static String IntenseQ() {
        Random random = new Random();
        long mostSigBits = random.nextLong();
        long leastSigBits = random.nextLong();
        UUID uuid = new UUID(mostSigBits, leastSigBits);
        return uuid.toString().replace("-", "");
    }

    public static void ReplaceT(Node OperTag_name) {
        for (int k = 0; k < OperTag_name.getAttributes().getLength(); k++) {
            String name = OperTag_name.getAttributes().item(k).getNodeName();
            String value_a = OperTag_name.getAttributes().item(k).getNodeValue();
            if ("current_ts".equals(name)) {
                value_a = value_a.replace("T", " ");
                ((Element) OperTag_name).setAttribute(name, value_a);
            } else {
                value_a = OperTag_name.getAttributes().item(k).getNodeValue();
            }

        }
    }

    public static String getPath(String fileNames) {
        String fullPath = System.getProperty("user.dir") + "\\xml_custom_process\\" + fileNames + ".yaml";
        return fullPath;
    }

    @Udf(description = "Convert XML_ATMCORE")

    public static   String cloneFM(
            @UdfParameter(value = "inputXML") String inputXML,
            @UdfParameter(value = "table_name") String tableName,
            @UdfParameter(value = "file_yaml") String fileName
    ) throws IOException, ParserConfigurationException, SAXException, TransformerException {
        try {
            String inputXMl = inputXML.trim().replaceFirst(".*?<", "<");
            //  String inputXMl =inputXML.replace("\u001c", "");
            System.out.println("inputXML : " + inputXMl);
            DocumentBuilderFactory db_factory = DocumentBuilderFactory.newInstance();
            db_factory.setCoalescing(true);
            DocumentBuilder builder = db_factory.newDocumentBuilder();
            Document document = builder.parse(new InputSource(new StringReader(inputXMl)));
            NodeList node_col = document.getElementsByTagName("col");
            NodeList opearations = document.getElementsByTagName("operation");
            Node OperTag = opearations.item(0);
            String rowop = IntenseQ();
            Element operation_tag = (Element) OperTag;
            operation_tag.setAttribute("IntenSeq", rowop);
            ReplaceT(OperTag);
            Yaml yaml = new Yaml();
            System.out.println(getPath("test1"));
            InputStream in = new FileInputStream(getPath(fileName));
            List<Map<String, Object>> data = yaml.load(in);
            for (int i = 0; i < node_col.getLength(); i++) {
                Element Col_element = (Element) node_col.item(i);
                String Col_name = Col_element.getAttribute("name");
                System.out.println("Col_name" + Col_name);
                for (Map<String, Object> tableEntry : data) {
                    Map<String, Object> tableInfo = (Map<String, Object>) tableEntry.get("table");
                    String table_name = (String) tableInfo.get("name");
                    if (tableName.equals(table_name)) {
                        List<Map<String, Object>> columns = (List<Map<String, Object>>) tableInfo.get("columns");
                        for (Map<String, Object> column : columns) {
                            String columnName = (String) column.get("name");
                            if (columnName.equals(Col_name)) {
                                String primaryKey = (String) column.get("primary_key");
                                String datatype = (String) column.get("datatype");
                                Element primary_key = document.createElement("primary_key");
                                primary_key.appendChild(document.createTextNode(primaryKey));
                                Col_element.appendChild(primary_key);
                                Element data_type = document.createElement("data_type");
                                data_type.appendChild(document.createTextNode(datatype));
                                Col_element.appendChild(document.createTextNode("\n"));
                                Col_element.appendChild(data_type);
                                Col_element.appendChild(document.createTextNode("\n"));
                            }
                        }
                    }
                }
            }
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
           // transformer.setOutputProperty(OutputKeys.INDENT,"yes");
            StringWriter writer = new StringWriter();
            transformer.transform(new DOMSource(document), new StreamResult(writer));
            String formatXML = writer.getBuffer().toString();
            System.out.println("output :  " + formatXML);
            return formatXML;
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    public static void main(String[] args) throws IOException, ParserConfigurationException, TransformerException, SAXException {
        String xml =  "\u0000\u0000\u0000\u0000\n<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<operation table=\"SACOM_SW_OWN.ATM_LOG\" type=\"I\" ts=\"2023-08-23 11:15:05.898835\" current_ts=\"2023-08-23T11:17:35.723001\" pos=\"00000000020001372473\" numCols=\"8\">\n  <col name=\"SHCLOG_ID\" index=\"0\">\n    <before missing=\"true\"/>\n    <after><![CDATA[AAEAsgAkZN52XQAB ]]></after>\n  </col>\n  <col name=\"INSTITUTION_ID\" index=\"1\">\n    <before missing=\"true\"/>\n    <after><![CDATA[1]]></after>\n  </col>\n  <col name=\"GROUP_NAME\" index=\"2\">\n    <before missing=\"true\"/>\n    <after><![CDATA[SGE5050101]]></after>\n  </col>\n  <col name=\"UNIT\" index=\"3\">\n    <before missing=\"true\"/>\n    <after><![CDATA[97]]></after>\n  </col>\n  <col name=\"FUNCTION_CODE\" index=\"4\">\n    <before missing=\"true\"/>\n    <after><![CDATA[200]]></after>\n  </col>\n  <col name=\"LOGGED_TIME\" index=\"5\">\n    <before missing=\"true\"/>\n    <after><![CDATA[2023-08-18 02:34:53.000000000]]></after>\n  </col>\n  <col name=\"LOG_DATA\" index=\"6\">\n    <before missing=\"true\"/>\n    <after><![CDATA[12097002033P20]]></after>\n  </col>\n  <col name=\"SITE_ID\" index=\"7\">\n    <before missing=\"true\"/>\n    <after><![CDATA[1]]></after>\n  </col>\n</operation>\n";
        String tableName = "ATMLOG";
        String fileName = "CARDDB_CARD_ATMLOG_v4";
        cloneFM(xml,tableName,fileName);

    }

}

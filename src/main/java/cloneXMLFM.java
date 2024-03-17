import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfParameter;
import org.apache.kafka.common.protocol.types.Field;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
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
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

public class cloneXMLFM {
    public static void cloneFM(
        @UdfParameter(value = "inputXML") String inputXML,
        @UdfParameter(value = "table_name") String tableName,
        @UdfParameter(value = "file_yaml") File file_yaml
    ) throws IOException, ParserConfigurationException, SAXException, TransformerException {
        try {
            String inputXMl = inputXML.trim().replaceFirst(".*?<", "<");
            System.out.println("inputXML : " + inputXMl);
            DocumentBuilderFactory db_factory = DocumentBuilderFactory.newInstance();
            db_factory.setCoalescing(true);
            DocumentBuilder builder = db_factory.newDocumentBuilder();
            Document document = builder.parse(new InputSource(new StringReader(inputXMl)));
            NodeList node_col = document.getElementsByTagName("col");
            InputStream in = new FileInputStream(file_yaml);
            Yaml yaml = new Yaml();
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
                                String datatype = (String) column.get("data_type");
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
            transformer.setOutputProperty(OutputKeys.INDENT, "no");
            transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
            StringWriter writer = new StringWriter();
            transformer.transform(new DOMSource(document), new StreamResult(writer));
            String formatXML = writer.getBuffer().toString();
            System.out.println("output :  " + formatXML);



            }
        catch(Exception e){
            e.printStackTrace();
            throw e;
        }
    }

    public static void main(String[] args) throws IOException, ParserConfigurationException, TransformerException, SAXException {
        String tableName =("ATM_LOG");
        File file_name = new File("E:\\XML_FORMATSC\\src\\main\\java\\test1.yaml");
        String xml ="\u0000\n<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<operation table=\"SACOM_SW_OWN.ATM_LOG\" type=\"I\" ts=\"2023-08-23 11:15:05.898835\" current_ts=\"2023-08-23T11:17:35.723001\" pos=\"00000000020001372473\" numCols=\"8\">\n  <col name=\"SHCLOG_ID\" index=\"0\">\n    <before missing=\"true\"/>\n    <after><![CDATA[AAEAsgAkZN52XQAB ]]></after>\n  </col>\n  <col name=\"INSTITUTION_ID\" index=\"1\">\n    <before missing=\"true\"/>\n    <after><![CDATA[1]]></after>\n  </col>\n  <col name=\"GROUP_NAME\" index=\"2\">\n    <before missing=\"true\"/>\n    <after><![CDATA[SGE5050101]]></after>\n  </col>\n  <col name=\"UNIT\" index=\"3\">\n    <before missing=\"true\"/>\n    <after><![CDATA[97]]></after>\n  </col>\n  <col name=\"FUNCTION_CODE\" index=\"4\">\n    <before missing=\"true\"/>\n    <after><![CDATA[200]]></after>\n  </col>\n  <col name=\"LOGGED_TIME\" index=\"5\">\n    <before missing=\"true\"/>\n    <after><![CDATA[2023-08-18 02:34:53.000000000]]></after>\n  </col>\n  <col name=\"LOG_DATA\" index=\"6\">\n    <before missing=\"true\"/>\n    <after><![CDATA[12097002033P20]]></after>\n  </col>\n  <col name=\"SITE_ID\" index=\"7\">\n    <before missing=\"true\"/>\n    <after><![CDATA[1]]></after>\n  </col>\n</operation>\n";
       cloneFM(xml,tableName,file_name);

    }



}

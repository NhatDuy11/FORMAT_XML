package UDF;
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
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
@UdfDescription(name = "format_XML_SC_FIX_v1", description = "Transform XML source")

public class FORMATXML_UDF {
    public  static String IntenseQ() {
        Random random = new Random();
        long mostSigBits = random.nextLong();
        long leastSigBits = random.nextLong();
        UUID uuid = new UUID(mostSigBits, leastSigBits);
        String lUUID = String.format("%040d", new BigInteger(uuid.toString().replace("-", ""), 16));
        return lUUID;
    }
//    public static void removeBeforeNodes(Node parent) {
//        NodeList childNodes = parent.getChildNodes();
//        for (int i = childNodes.getLength() - 1; i >= 0; i--) {
//            Node childNode = childNodes.item(i);
//            if (childNode.getNodeType() == Node.ELEMENT_NODE && childNode.getNodeName().equals("before")) {
//                parent.removeChild(childNode);
//            }
//        }
//    }

    public static  void ReplaceT(Node OperTag_name) {
        for (int k = 0 ; k<OperTag_name.getAttributes().getLength();k++) {
            String name = OperTag_name.getAttributes().item(k).getNodeName();
           // System.out.println("name : " + name);
            String value_a = OperTag_name.getAttributes().item(k).getNodeValue();
            if ( "current_ts".equals(name)) {
                value_a =value_a.replace("T"," ");
                ((Element) OperTag_name).setAttribute(name, value_a);
            }
            else {
                value_a =OperTag_name.getAttributes().item(k).getNodeValue();
            }

        }
    }

    @Udf(description = "Convert XML")

    public   String Result_XML(@UdfParameter(value = "xmlInput")String inputXML,@UdfParameter(value ="table_name") String tableName) throws ParserConfigurationException, IOException, TransformerException, SAXException {
        System.setProperty("org.apache.xalan.processor.TransformerFactoryImpl",
                "com.xyz.YourFactory");
        try {
            String xmlinput_str = inputXML.trim().replaceFirst(".*?<", "<");
            System.out.println("input :  " + inputXML);
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setCoalescing(true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(new InputSource(new StringReader(xmlinput_str)));
            NodeList colElements = document.getElementsByTagName("col");


            InputStream in = new FileInputStream("/opt/confluent-7.0.1/extractjson/table_ATMLOG.yaml");
            //InputStream in = new FileInputStream("E:\\XML_FORMATSC\\src\\main\\java\\test.yaml");

            Yaml yaml = new Yaml();
            Map<String, Object> data = yaml.load(in);
            List<Map<String, Object>> tables = (List<Map<String, Object>>) ((List<Map<String, Object>>) data.get("database"));
            for (int i = 0; i < colElements.getLength(); i++) {
                Element Colement = (Element) colElements.item(i);
                String colName = Colement.getAttribute("name");
//                Node Before_node = colElements.item(i);
//                removeBeforeNodes(Before_node.getParentNode());


//            String colValue = Colement.getElementsByTagName("after").item(0).getTextContent();
//            Colement.appendChild(document.createElement("after").setTextContent(colValue));

                for (Map<String, Object> table : tables) {
                    Map<String, Object> tmpMapping = table;
                    Map<String, Object> tableMetadata = (Map<String, Object>) tmpMapping.get("table");
                    String metaName = (String) tableMetadata.get("name");
                    if (tableName.equals(metaName)) {
                        List<Map<String, Object>> columns = (List<Map<String, Object>>) tableMetadata.get("columns");
                        for (Map<String, Object> column : columns) {
                            String columnName = (String) column.get("name");
                            if (columnName.equals(colName)) {
                                String primaryKey = (String) column.get("primary_key");
                                String datatype = (String) column.get("data_type");
                                Element primary_key = document.createElement("primary_key");
                                primary_key.appendChild(document.createTextNode(primaryKey));

                                Colement.appendChild(primary_key);
                                Element data_type = document.createElement("data_type");
                                data_type.appendChild(document.createTextNode(datatype));
                                Colement.appendChild(document.createTextNode("\n"));
                                Colement.appendChild(data_type);
                                Colement.appendChild(document.createTextNode("\n"));

                            }
                            NodeList opearations = document.getElementsByTagName("operation");
                            Node OperTag = opearations.item(0);
                            String rowop = IntenseQ();
                            Element operation_tag = (Element) OperTag;
                            operation_tag.setAttribute("IntenSeq", rowop);
                            ReplaceT(OperTag);

                      //          NodeList before_after = document.getElementsByTagName("before");
//                            Node Before = before_after.item(0);
//                            RemoveBefore(Before);



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
            System.out.println("output :  "+formatXML);
            return  formatXML;
        //   return formatXML;


        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    public static void main(String[] args) throws ParserConfigurationException, IOException, TransformerException, SAXException {
//        String tableName =("ATM_LOG");
//        String xml ="\u0000\n<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<operation table=\"SACOM_SW_OWN.ATM_LOG\" type=\"I\" ts=\"2023-08-23 11:15:05.898835\" current_ts=\"2023-08-23T11:17:35.723001\" pos=\"00000000020001372473\" numCols=\"8\">\n  <col name=\"SHCLOG_ID\" index=\"0\">\n    <before missing=\"true\"/>\n    <after><![CDATA[AAEAsgAkZN52XQAB ]]></after>\n  </col>\n  <col name=\"INSTITUTION_ID\" index=\"1\">\n    <before missing=\"true\"/>\n    <after><![CDATA[1]]></after>\n  </col>\n  <col name=\"GROUP_NAME\" index=\"2\">\n    <before missing=\"true\"/>\n    <after><![CDATA[SGE5050101]]></after>\n  </col>\n  <col name=\"UNIT\" index=\"3\">\n    <before missing=\"true\"/>\n    <after><![CDATA[97]]></after>\n  </col>\n  <col name=\"FUNCTION_CODE\" index=\"4\">\n    <before missing=\"true\"/>\n    <after><![CDATA[200]]></after>\n  </col>\n  <col name=\"LOGGED_TIME\" index=\"5\">\n    <before missing=\"true\"/>\n    <after><![CDATA[2023-08-18 02:34:53.000000000]]></after>\n  </col>\n  <col name=\"LOG_DATA\" index=\"6\">\n    <before missing=\"true\"/>\n    <after><![CDATA[12097002033P20]]></after>\n  </col>\n  <col name=\"SITE_ID\" index=\"7\">\n    <before missing=\"true\"/>\n    <after><![CDATA[1]]></after>\n  </col>\n</operation>\n";
//        Result_XML(xml,tableName);

    }

    }
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.yaml.snakeyaml.Yaml;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.*;

@UdfDescription(name = "xml_custom",description = "add tag for xml")
public class FORMAT_XML_SACOM_backup {
    public static String yamlDir = "/opt/confluent/xml_table_yaml_format/";
    public static String logDir = "/apps/log/confluent/ksql/";
    //  public static String yamlDir = "E:\\opt\\confluent\\xml_table_yaml_format\\xml_custom_process\\";
    //  public static String logDir = "E:\\apps\\log\\ksql\\";
    public static String IntentseQ() {
        String lUUID = String.format("%040d", new BigInteger(UUID.randomUUID().toString().replace("-", ""), 16));
        return lUUID;
    }


  //  public static String yamlDir = "E:\\opt\\confluent\\xml_table_yaml_format\\xml_custom_process\\";
  //  public static String logDir = "E:\\apps\\log\\ksql\\";
    public static void ReplaceT(Node OperTag_name) {
        for (int k = 0; k < OperTag_name.getAttributes().getLength(); k++) {
            String name = OperTag_name.getAttributes().item(k).getNodeName();
            String value_a = OperTag_name.getAttributes().item(k).getNodeValue();
            if ("current_ts".equals(name)) {
                value_a = value_a.replace("T", " ");
                ((Element) OperTag_name).setAttribute(name, value_a);
            }
        }
    }
    @Udf(description = "add tag for xml format")
    public   String format_xml(
            @UdfParameter(value = "inputXML") String inputXML,
            @UdfParameter(value = "table_name") String tableName,
            @UdfParameter(value = "file_yaml") String fileName
    ) throws Exception {
        try {
           // String inputXMl = inputXML.trim().replaceFirst(".*?<", "<");
            String inputXMl =inputXML.replaceAll("[\u0000-\u001F]", "");
          //  System.out.println("inputXML : " + inputXMl);
            DocumentBuilderFactory db_factory = DocumentBuilderFactory.newInstance();
            db_factory.setCoalescing(true);
            DocumentBuilder builder = db_factory.newDocumentBuilder();
            Document document = builder.parse(new InputSource(new StringReader(inputXMl)));
            NodeList node_col = document.getElementsByTagName("col");
            NodeList opearations = document.getElementsByTagName("operation");
            Node rowNode = opearations.item(0);
            Element rowOpElement = document.createElement("rowOp");
            String rowOpAttr = IntentseQ();
            rowOpElement.setAttribute("intentSEQ",rowOpAttr);
            rowNode.getParentNode().replaceChild(rowOpElement,rowNode);
            rowOpElement.appendChild(rowNode);
            Element msgElement = document.createElement("msg");
            msgElement.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
            msgElement.setAttribute("xsi:noNamespaceSchemaLocation", "mqcap.xsd");
            rowOpElement.getParentNode().replaceChild(msgElement,rowOpElement);
            msgElement.appendChild(rowOpElement);
            ReplaceT(rowNode);
            document.renameNode(rowNode,null,"Row");
            Yaml yaml = new Yaml();
           // System.out.println(getPath("test1"));
            String fullpathYaml = yamlDir+fileName+".yaml";
            InputStream in = new FileInputStream(fullpathYaml);
            List<Map<String, Object>> data = yaml.load(in);
            for (int i = 0; i < node_col.getLength(); i++) {
                Element Col_element = (Element) node_col.item(i);
                NodeList childNodes = Col_element.getElementsByTagName("before");
                System.out.println( "before : "  + Col_element.getElementsByTagName("before").item(0));
                if (childNodes.item(0) != null){
                    Col_element.removeChild(childNodes.item(0));
                }
                Col_element.removeAttribute("index");
                String Col_name = Col_element.getAttribute("name");
              //  System.out.println("Col_name" + Col_name);
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
            //transformer.setOutputProperty(OutputKeys.INDENT,"yes");
            StringWriter writer = new StringWriter();
            transformer.transform(new DOMSource(document), new StreamResult(writer));
            String formatXML = writer.getBuffer().toString();
         //   System.out.println("output :  " + formatXML);
            return formatXML;
        } catch (Exception e) {
            writeErrorToFile(e,inputXML,tableName);
            throw e;
        }

    }
    public static void writeErrorToFile(Exception a,String xmlInput, String tableName) throws Exception {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
        String fileName= logDir + format.format(Calendar.getInstance().getTime()) + "_"  + tableName +  ".txt";
        try (PrintWriter writer = new PrintWriter(new FileWriter(fileName, true))) {
            writer.println("Error occurred at: " + new Date());
            writer.println("Xml input error : " + xmlInput);
            a.printStackTrace(writer);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    public static void main(String[] args) throws Exception {
//        String xml = "<?xml version='1.0' encoding='UTF-8'?> <operation table='SACOM_SW_OWN.ATMCASSETTECFG' type='U' ts='2024-03-02 17:34:36.000000' current_ts='2024-03-02T17:34:41.472000' pos='00000000140000025111' numCols='8'> \n" +
//                "<col name='INSTITUTIONID' index='0'> <before><![CDATA[1]]></before> <after><![CDATA[1]]></after> </col> \n" +
//                "<col name='CASSETTE_CFG_ID' index='1'> <before><![CDATA[2]]></before> <after><![CDATA[2]]></after> </col> \n" +
//                "<col name='CASSETTE_NBR' index='2'> <before><![CDATA[1]]></before> <after><![CDATA[1]]></after> </col> \n" +
//                "<col name='ITEM_TYPE' index='3'> <missing/> </col> \n" +
//                "<col name='DENOMINATION' index='4'> <before><![CDATA[500000]]></before> <after><![CDATA[1]]></after> </col> \n" +
//                "<col name='CURRENCY' index='5'> <missing/> </col> \n" +
//                "<col name='DENOMINATIONID' index='6'> <missing/> </col> \n" +
//                "<col name='MAXDISPENSE' index='7'> <missing/> </col> </operation>";
//        String tableName = "ATMCASSETTECFG";
//        String fileName = "ISTUAT_SACOM_SW_OWN_ATMCASSETTECFG";
//        format_xml(xml,tableName,fileName);
//
//    }
}

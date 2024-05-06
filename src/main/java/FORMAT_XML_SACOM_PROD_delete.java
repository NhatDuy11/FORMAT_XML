import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import org.apache.commons.text.StringEscapeUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.yaml.snakeyaml.Yaml;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.*;


@UdfDescription(name = "xml_custom",description = "add tag for xml new")
public class FORMAT_XML_SACOM_PROD_delete {
      //public static String yamlDir = "/opt/confluent/xml_table_yaml_format/";
    // public static String logDir = "/data/confluent/error_ext/";
    //public static String logDir = "/apps/log/confluent/ksql/";
    public static String yamlDir = "E:\\opt\\confluent\\xml_table_yaml_format\\xml_custom_process\\";
   public static String logDir = "E:\\apps\\log\\ksql\\";

    public static String IntentseQ() {
        String lUUID = String.format("%040d", new BigInteger(UUID.randomUUID().toString().replace("-", ""), 16));
        return lUUID;
    }
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
    @Udf(description = "add tag for xml format new")
    public  static      String format_xml(
            @UdfParameter(value = "inputXML") String inputXML,
            @UdfParameter(value = "table_name") String tableName,
            @UdfParameter(value = "file_yaml") String fileName
    ) throws Exception {
        try {
            inputXML = inputXML.replaceFirst("\n<operation", "<operation");
            inputXML=inputXML.replaceAll("</operation>\n", "</operation>");

            String inputXMLStandard =StringEscapeUtils.escapeJava(inputXML);
             //String inputXMLStandard =inputXML.replaceAll("[\u0000-\u001F]", "");
           // String inputXMLStandard =inputXML.replaceAll("<!\\[CDATA\\[(.*?)\\]\\]>", "$1");;
            DocumentBuilderFactory db_factory = DocumentBuilderFactory.newInstance();
            db_factory.setCoalescing(true);
            DocumentBuilder builder = db_factory.newDocumentBuilder();
            /*convert string to xml object*/
            Document document = builder.parse(new InputSource(new StringReader(inputXMLStandard)));
            /* get node operation , set name = rowNode to prepare rename to Row .
             * get item= 0 because only one tag <operation>*/
            Node rowNode = document.getElementsByTagName("operation").item(0);
            /* get dml type of row, if dmlType in (D: delete, I: Insert, U: Update */
            String dmlType = rowNode.getAttributes().getNamedItem("type").getNodeValue();
            /* add rowOp node and attribute of it*/
            Element rowOpElement = document.createElement("rowOp");
            String rowOpAttr = IntentseQ();
            rowOpElement.setAttribute("intentSEQ",rowOpAttr);
            /*replace  rowNode to rowOpElement */
            rowNode.getParentNode().replaceChild(rowOpElement,rowNode);
            /* add rowNode to child of rowOpElement*/
            rowOpElement.appendChild(rowNode);
            /* add msg node and attribute of it */
            Element msgElement = document.createElement("msg");
            msgElement.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
            msgElement.setAttribute("xsi:noNamespaceSchemaLocation", "mqcap.xsd");
            /*replace  rowOpElement to msgElement */
            rowOpElement.getParentNode().replaceChild(msgElement,rowOpElement);
            /* add rowOpElement to child of msgElement*/
            msgElement.appendChild(rowOpElement);
            /* remove T character in attribute current_ts*/
            ReplaceT(rowNode);
            /*Rename tag <operation> to <Row>*/
            document.renameNode(rowNode,null,"Row");
            /* create yaml object*/
            Yaml yaml = new Yaml();
            String fullpathYaml = yamlDir+fileName+".yaml";
            InputStream yamlTemplate = new FileInputStream(fullpathYaml);
            List<Map<String, Object>> templateData = yaml.load(yamlTemplate);
            /*cause list templateData contain is one table so get(0) to get the table */
            Map<String, Object> templateTableInfo = (Map<String, Object>) templateData.get(0).get("table");
            String templateTableName =  templateTableInfo.get("name").toString();
            List<Map<String, Object>> listTemplateColumns = (List<Map<String, Object>>) templateTableInfo.get("columns");
            /*get list tag <col> */
            NodeList colNode = document.getElementsByTagName("col");

            /* process every tag <col>*/
            for (int i = 0; i < colNode.getLength(); i++) {
                Element colElement = (Element) colNode.item(i);
                /*get child node <before> and <after> */
                Node childNodesBefore = colElement.getElementsByTagName("before").item(0);


                Node childNodesAfter = colElement.getElementsByTagName("after").item(0);
                //String getCDATA = childNodesAfter.getAttributes().item()
                /*only process when childNodesBefore <> null */
                if (childNodesBefore != null){
                    /*if DML type =Delete then remove afterNode because afterNode is null when delete
                     * and rename beforeNode to afterNode
                     * else only remove beforeNode ,retain afterNode */
                    if (dmlType.equals("D")) {
                        colElement.removeChild(childNodesAfter);
                        document.renameNode(childNodesBefore,null,"after");
                    }
                    else {
                        colElement.removeChild(childNodesBefore);
                    }
                }

                /*remove attribute index*/
                colElement.removeAttribute("index");
                String colName = colElement.getAttribute("name");
                /*with every tag <col> add 2 child tag : <primary_key> and <data_type> base on template yaml*/
                if (tableName.equals(templateTableName)) {
                    for (Map<String, Object> templateColumn : listTemplateColumns) {
                        String columnName =  templateColumn.get("name").toString();
                        if (columnName.equals(colName)) {

                            String primaryKey = templateColumn.get("primary_key").toString();
                            String datatype =  templateColumn.get("data_type").toString();
                            Element primary_key = document.createElement("primary_key");
                            primary_key.appendChild(document.createTextNode(primaryKey));
                            colElement.appendChild(primary_key);
                            Element data_type = document.createElement("data_type");
                            data_type.appendChild(document.createTextNode(datatype));colElement.appendChild(document.createTextNode("\n"));
                            colElement.appendChild(data_type);
                            colElement.appendChild(document.createTextNode("\n"));
                        }
                    }
                }
            }
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT,"yes");
            StringWriter writer = new StringWriter();
            transformer.transform(new DOMSource(document), new StreamResult(writer));
            String formatXML = writer.getBuffer().toString();
            String formatXMLResult = StringEscapeUtils.unescapeJava(formatXML);

            System.out.println("output :  " + formatXML);
           System.out.println("fomrat_XML1" + formatXMLResult);
            return formatXMLResult;
            //return formatXML;
        } catch (Exception e) {
            e.printStackTrace();
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

    public static void main(String[] args) throws Exception {

        String xml ="<?xml version='1.0' encoding='UTF-8'?><operation table='SACOM_SW_OWN.ATM_LOG' type='I' ts='2024-03-15 13:58:41.000000' current_ts='2024-03-15T13:58:45.241000' pos='00000000230003055451' numCols='8'>\n <col name='SHCLOG_ID' index='0'>\n   <before missing='true'/>\n   <after><![CDATA[AAEAA2YoZfPxoQAA    ]]></after>\n </col>\n <col name='INSTITUTION_ID' index='1'>\n   <before missing='true'/>\n   <after><![CDATA[2]]></after>\n </col>\n <col name='GROUP_NAME' index='2'>\n   <before missing='true'/>\n   <after><![CDATA[CAM10521]]></after>\n </col>\n <col name='UNIT' index='3'>\n   <before missing='true'/>\n   <after><![CDATA[1]]></after>\n </col>\n <col name='FUNCTION_CODE' index='4'>\n   <before missing='true'/>\n   <after><![CDATA[200]]></after>\n </col>\n <col name='LOGGED_TIME' index='5'>\n   <before missing='true'/>\n   <after><![CDATA[2024-03-15 13:58:41]]></after>\n </col>\n <col name='LOG_DATA' index='6'>\n   <before missing='true'/>\n   <after><![CDATA[22\u001c001001340\u001c\u001c9\u001dCAM\u001d9F2701009F2608F8AC1C38BE8450D39F10070600120320BC00950580808000009B0260009F3303604020]]></after>\n </col>\n <col name='SITE_ID' index='7'>\n   <before missing='true'/>\n   <after><![CDATA[1]]></after>\n </col>\n</operation>";
        String tableName = "ATM_LOG";
        String fileName = "ISTUAT_SACOM_SW_OWN_ATM_LOG";
        format_xml(xml,tableName,fileName);

    }
}

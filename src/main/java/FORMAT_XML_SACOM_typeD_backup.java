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
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.*;

@UdfDescription(name = "xml_custom",description = "add tag for xmltype")
public class FORMAT_XML_SACOM_typeD_backup {
    public static String yamlDir = "/opt/confluent/xml_table_yaml_format/";
    public static String logDir = "/apps/log/confluent/ksql/";
    //  public static String yamlDir = "E:\\opt\\confluent\\xml_table_yaml_format\\xml_custom_process\\";
    //  public static String logDir = "E:\\apps\\log\\ksql\\";

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
    @Udf(description = "add tag for xmltype format")
    public   String format_xml(
            @UdfParameter(value = "inputXML") String inputXML,
            @UdfParameter(value = "table_name") String tableName,
            @UdfParameter(value = "file_yaml") String fileName
    ) throws Exception {
        try {
            String inputXMLStandard =inputXML.replaceAll("[\u0000-\u001F]", "");
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
                            data_type.appendChild(document.createTextNode(datatype));
                            //colElement.appendChild(document.createTextNode("\n"));
                            colElement.appendChild(data_type);
                            //colElement.appendChild(document.createTextNode("\n"));
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
            System.out.println("output :  " + formatXML);
            return formatXML;
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

    // public static void main(String[] args) throws Exception {
    //     String xml = "<?xml version='1.0' encoding='UTF-8'?><operation table='SACOM_SW_OWN.ATM' type='D' ts='2024-03-09 10:45:37.000000' current_ts='2024-03-09T10:45:41.692000' pos='00000000230000004852' numCols='41'><col name='UNIT' index='0'><before><![CDATA[212]]></before><after missing='true'/></col><col name='INSTITUTIONID' index='1'><before><![CDATA[1]]></before><after missing='true'/></col><col name='GROUP_NAME' index='2'><before><![CDATA[SGB5050101]]></before><after missing='true'/></col><col name='GOSERVICE' index='3'><before><![CDATA[Y]]></before><after missing='true'/></col><col name='MODEL' index='4'><before><![CDATA[5886]]></before><after missing='true'/></col><col name='MAKE' index='5'><before><![CDATA[NDC]]></before><after missing='true'/></col><col name='LOCAL_PIN' index='6'><before><![CDATA[N]]></before><after missing='true'/></col><col name='ADD_TOTAL' index='7'><before><![CDATA[Y]]></before><after missing='true'/></col><col name='TXN_ALLOW' index='8'><before><![CDATA[YYYYY ]]></before><after missing='true'/></col><col name='CURRENCY' index='9'><before><![CDATA[704]]></before><after missing='true'/></col><col name='LOCATION' index='10'><before><![CDATA[000000099999999 ]]></before><after missing='true'/></col><col name='LOGGING' index='11'><before><![CDATA[Y]]></before><after missing='true'/></col><col name='GEO_LOC' index='12'><before><![CDATA[70400000 ]]></before><after missing='true'/></col><col name='ACCEPTORNAME' index='13'><before><![CDATA[7102-KAFKA TEST 20240308 160200 VN]]></before><after missing='true'/></col><col name='TERMID' index='14'><before><![CDATA[00007102]]></before><after missing='true'/></col><col name='O_ROWID' index='15'><before isNull='true'/><after missing='true'/></col><col name='H_LOCATION' index='16'><before isNull='true'/><after missing='true'/></col><col name='WD_NO_RPT' index='17'><before><![CDATA[1]]></before><after missing='true'/></col><col name='TIME_OFFSET' index='18'><before><![CDATA[0]]></before><after missing='true'/></col><col name='MAC_REQUIRED' index='19'><before><![CDATA[N]]></before><after missing='true'/></col><col name='MAC_NCR_TXNREQ' index='20'><before isNull='true'/><after missing='true'/></col><col name='MAC_NCR_TXNREPLY' index='21'><before isNull='true'/><after missing='true'/></col><col name='MAC_NCR_SOLITSTAT' index='22'><before isNull='true'/><after missing='true'/></col><col name='MAC_NCR_OTHER' index='23'><before isNull='true'/><after missing='true'/></col><col name='MAC_NCR_TRACK1' index='24'><before isNull='true'/><after missing='true'/></col><col name='MAC_NCR_TRACK2' index='25'><before isNull='true'/><after missing='true'/></col><col name='MAC_NCR_TRACK3' index='26'><before isNull='true'/><after missing='true'/></col><col name='FUNC_FLAGS' index='27'><before><![CDATA[NYANNNNNNNNNNNNN ]]></before><after missing='true'/></col><col name='MAXDISPENSE' index='28'><before><![CDATA[40]]></before><after missing='true'/></col><col name='TERM_COMP' index='29'><before isNull='true'/><after missing='true'/></col><col name='HDL_ACK' index='30'><before><![CDATA[N]]></before><after missing='true'/></col><col name='MAC_NCR_EMV' index='31'><before isNull='true'/><after missing='true'/></col><col name='PRIORITY_ROUTING' index='32'><before isNull='true'/><after missing='true'/></col><col name='INTERNAL_UNIT' index='33'><before><![CDATA[26]]></before><after missing='true'/></col><col name='FIT_LIST_ID' index='34'><before><![CDATA[4]]></before><after missing='true'/></col><col name='CASSETTE_CFG_ID' index='35'><before><![CDATA[21]]></before><after missing='true'/></col><col name='DISPENSE_TABLE_ID' index='36'><before><![CDATA[17]]></before><after missing='true'/></col><col name='MANUFACTURER' index='37'><before><![CDATA[NCR]]></before><after missing='true'/></col><col name='EMULATION' index='38'><before><![CDATA[NDC]]></before><after missing='true'/></col><col name='MAX_COIN_DISPENSE' index='39'><before isNull='true'/><after missing='true'/></col><col name='RKM_INST' index='40'><before isNull='true'/><after missing='true'/></col></operation>\n";
    //     String tableName = "ATM";
    //     String fileName = "ISTUAT_SACOM_SW_OWN_ATM";
    //     format_xml(xml,tableName,fileName);

    // }
}

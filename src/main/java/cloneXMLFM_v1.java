import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
@UdfDescription(name = "ATM_CORE_V1", description = "Transform XML source_ATM_CORE")

public class cloneXMLFM_v1 {
    @Udf(description = "Convert XML_ATMCORE")

    public String cloneFM(
        @UdfParameter(value = "inputXML") String inputXML,
        @UdfParameter(value = "table_name") String tableName,
        @UdfParameter(value = "file_yaml")  final List<String>  file_yaml
    ) throws IOException, ParserConfigurationException, SAXException, TransformerException {
        try {
            //String inputXMl = inputXML.trim().replaceFirst(".*?<", "<");
            String inputXMl =inputXML.replace("\u001c", "");

            System.out.println("inputXML : " + inputXMl);
            DocumentBuilderFactory db_factory = DocumentBuilderFactory.newInstance();
            db_factory.setCoalescing(true);
            DocumentBuilder builder = db_factory.newDocumentBuilder();
            Document document = builder.parse(new InputSource(new StringReader(inputXMl)));
            NodeList node_col = document.getElementsByTagName("col");
            Yaml yaml = new Yaml();
            for(String yamls:file_yaml){
                InputStream in = new FileInputStream(yamls);
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
                                   // Col_element.appendChild(document.createTextNode("\n"));
                                    Col_element.appendChild(data_type);
                                   // Col_element.appendChild(document.createTextNode("\n"));
                                }


                            }


                        }


                    }


                }


            }

            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
            StringWriter writer = new StringWriter();
            transformer.transform(new DOMSource(document), new StreamResult(writer));
            String formatXML = writer.getBuffer().toString();
            System.out.println("output :  " + formatXML);
            return  formatXML;

            }
        catch(Exception e){
            e.printStackTrace();
            throw e;
        }
    }

    public static void main(String[] args) throws IOException, ParserConfigurationException, TransformerException, SAXException {
//        String tableName =("ATM_LOG");
//        List<String> file_name = new ArrayList<>();
//        file_name.add("E:\\XML_FORMATSC\\src\\main\\java\\test1.yaml");
//     //   file_name.add("E:\\XML_FORMATSC\\src\\main\\java\\test1.yaml");
//        String xml = "<?xml version='1.0' encoding='UTF-8'?><operation table='SACOM_SW_OWN.ATM_LOG' type='I' ts='2023-08-23 11:15:05.898835' current_ts='2023-08-23T11:17:35.723001' pos='00000000020001372473' numCols='8'><col name='SHCLOG_ID' index='0'><before missing='true'/><after><![CDATA[AAEAsgAkZN52XQAB ]]></after></col><col name='INSTITUTION_ID' index='1'><before missing='true'/><after><![CDATA[1]]></after></col><col name='GROUP_NAME' index='2'><before missing='true'/><after><![CDATA[SGE5050101]]></after></col><col name='UNIT' index='3'><before missing='true'/><after><![CDATA[97]]></after></col><col name='FUNCTION_CODE' index='4'><before missing='true'/><after><![CDATA[200]]></after></col><col name='LOGGED_TIME' index='5'><before missing='true'/><after><![CDATA[2023-08-18 02:34:53.000000000]]></after></col><col name='LOG_DATA' index='6'><before missing='true'/><after><![CDATA[12\u001C097002033\u001C\u001CP20]]></after></col><col name='SITE_ID' index='7'><before missing='true'/><after><![CDATA[1]]></after></col></operation> ";
//
//        cloneFM(xml,tableName,file_name);

    }



}
